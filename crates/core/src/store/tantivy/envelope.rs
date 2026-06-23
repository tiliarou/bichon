//
// Copyright (c) 2025-2026 rustmailer.com (https://rustmailer.com)
//
// This file is part of the Bichon Email Archiving Project
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
    path::PathBuf,
    sync::{Arc, LazyLock},
    time::Duration,
};

use crate::{
    account::{migration::AccountModel, stats::AccountStats},
    common::{paginated::DataPage, signal::SIGNAL_MANAGER},
    dashboard::{DashboardStats, Group, LargestEmail, TimeBucket},
    error::{code::ErrorCode, BichonResult},
    message::{
        search::{EmailSearchFilter, SortBy},
        tags::{TagAction, TagCount, TagsRequest},
    },
    raise_error,
    settings::dir::DATA_DIR_MANAGER,
    store::{
        blob::BLOB_MANAGER,
        envelope::Envelope,
        tantivy::{
            attachment::ATTACHMENT_MANAGER,
            dedup_cache::DEDUP_CACHE,
            fatal_commit,
            fields::{
                F_ACCOUNT_ID, F_DATE, F_FROM, F_ID, F_INGEST_AT, F_INTERNAL_DATE,
                F_REGULAR_ATTACHMENT_COUNT, F_SIZE, F_TAGS, F_THREAD_ID, F_UID,
            },
            model::{extract_contacts, EnvelopeWithAttachments},
            schema::SchemaTools,
            tokenizers::EuroTokenizer,
        },
    },
    utc_now,
    utils::html::extract_text,
};

use chrono::Utc;
use mail_parser::MessageParser;
use serde_json::json;
use tantivy::{
    aggregation::{
        agg_req::Aggregations,
        agg_result::{
            AggregationResult, AggregationResults, BucketEntries, BucketResult, MetricResult,
        },
        AggregationCollector, Key,
    },
    collector::{Count, DocSetCollector, FacetCollector, TopDocs},
    indexer::{LogMergePolicy, UserOperation},
    query::{AllQuery, BooleanQuery, EmptyQuery, Occur, Query, QueryParser, RangeQuery, TermQuery},
    schema::{IndexRecordOption, Value},
    DocAddress, Index, IndexReader, IndexWriter, Order, TantivyDocument, Term,
};
use tantivy::{schema::Facet, Searcher};
use tokio::{
    sync::{mpsc, Mutex},
    task::{self, JoinHandle},
};
use tracing::{info, warn};

pub static ENVELOPE_MANAGER: LazyLock<IndexManager> = LazyLock::new(IndexManager::new);

pub struct IndexManager {
    index: Arc<Index>,
    index_writer: Arc<Mutex<IndexWriter>>,
    sender: mpsc::Sender<TantivyDocument>,
    reader: IndexReader,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl IndexManager {
    pub(crate) fn index_writer(&self) -> &Arc<Mutex<IndexWriter>> {
        &self.index_writer
    }

    pub(crate) fn create_reader(&self) -> BichonResult<IndexReader> {
        self.index
            .reader()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))
    }

    pub async fn shutdown(&self) {
        let mut guard = self.handle.lock().await;
        if let Some(handle) = guard.take() {
            let _ = handle.await;
        }
    }
    pub fn new() -> Self {
        let index = Self::open_or_create_index(&DATA_DIR_MANAGER.envelope_dir);
        index.tokenizers().register("euro", EuroTokenizer::new());
        let mut merge_policy = LogMergePolicy::default();
        merge_policy.set_min_num_segments(25);
        merge_policy.set_min_layer_size(10_000);
        merge_policy.set_max_docs_before_merge(100_000);

        let index_writer = index
            .writer_with_num_threads(4, 67_108_864)
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to create IndexWriter with 4 threads and 64MB buffer for {:?}: {}",
                    &DATA_DIR_MANAGER.envelope_dir, e
                )
            });
        index_writer.set_merge_policy(Box::new(merge_policy));
        let index_writer = Arc::new(Mutex::new(index_writer));
        let reader = index.reader().unwrap_or_else(|e| {
            panic!(
                "Failed to create IndexReader for {:?}: {}",
                &DATA_DIR_MANAGER.envelope_dir, e
            )
        });

        let (sender, mut receiver) = mpsc::channel::<TantivyDocument>(100);

        let writer = index_writer.clone();
        let handler = task::spawn(async move {
            let mut shutdown = SIGNAL_MANAGER.subscribe();
            let mut commit_interval = tokio::time::interval(Duration::from_secs(60));
            let mut pending_count = 0;
            let commit_threshold = 1000;
            loop {
                tokio::select! {
                    maybe_msg = receiver.recv() => {
                        match maybe_msg {
                            Some(doc) => {
                                let mut writer = writer.lock().await;
                                let mut batch_count = 0;
                                match writer.add_document(doc) {
                                    Ok(_) => {
                                        batch_count += 1;
                                    }
                                    Err(e) => {
                                        eprintln!("[ERROR] Failed to add document: {e:?}");
                                        tracing::error!("Tantivy: Failed to add document: {e:?}");
                                    }
                                }
                                while let Ok(next_doc) = receiver.try_recv() {
                                    match writer.add_document(next_doc) {
                                        Ok(_) => batch_count += 1,
                                        Err(e) => {
                                            eprintln!("[ERROR] Failed to add document: {e:?}");
                                            tracing::error!("Tantivy: Failed to add document: {e:?}");
                                        }
                                    }
                                }
                                if batch_count > 0 {
                                    pending_count += batch_count;
                                }
                                if pending_count >= commit_threshold {
                                    tracing::info!(
                                        "Tantivy: Reached threshold ({} docs), committing...",
                                        pending_count
                                    );
                                    tokio::task::block_in_place(|| fatal_commit(&mut writer));
                                    tracing::debug!(
                                        "Tantivy: committed {} docs, pending reset to 0",
                                        pending_count
                                    );
                                    pending_count = 0;
                                    commit_interval.reset();
                                }
                            }
                            None => {
                                tracing::info!("Tantivy: Receiver closed. Finalizing...");
                                if pending_count > 0 {
                                    let mut writer = writer.lock().await;
                                    tokio::task::block_in_place(|| fatal_commit(&mut writer));
                                }
                                break;
                            },
                        }
                    }
                    _ = commit_interval.tick() => {
                        if pending_count > 0 {
                            let mut writer = writer.lock().await;
                            tracing::debug!(
                                "Tantivy: periodic commit ({} docs pending)",
                                pending_count
                            );
                            tokio::task::block_in_place(|| fatal_commit(&mut writer));
                            pending_count = 0;
                        }
                    }
                    _ = shutdown.recv() => {
                        tracing::info!("Tantivy: Shutdown signal received. Performing final commit...");
                        if pending_count > 0 {
                            let mut writer = writer.lock().await;
                            tokio::task::block_in_place(|| fatal_commit(&mut writer));
                        }
                        tracing::info!("Tantivy: Shutdown cleanup complete.");
                        break;
                    }
                }
            }
        });
        Self {
            index: Arc::new(index),
            index_writer,
            sender,
            reader,
            handle: Mutex::new(Some(handler)),
        }
    }

    pub async fn queue(&self, doc: TantivyDocument) {
        if let Err(e) = self.sender.send(doc).await {
            tracing::warn!(error = %e, "Failed to queue document into Tantivy writer channel");
        }
    }

    fn open_or_create_index(index_dir: &PathBuf) -> Index {
        let need_create = !index_dir.exists()
            || index_dir
                .read_dir()
                .map(|mut d| d.next().is_none())
                .unwrap_or(true);
        if need_create {
            info!(
                "Email index not found or empty, creating new index at {}",
                index_dir.display()
            );
            std::fs::create_dir_all(&index_dir).unwrap_or_else(|e| {
                panic!("Failed to create index directory {:?}: {}", index_dir, e)
            });
            Index::create_in_dir(&index_dir, SchemaTools::email_schema())
                .unwrap_or_else(|e| panic!("Failed to create index in {:?}: {}", index_dir, e))
        } else {
            info!("Opening existing email index at {}", index_dir.display());
            Self::open(&index_dir)
        }
    }

    fn open(index_dir: &PathBuf) -> Index {
        Index::open_in_dir(index_dir)
            .unwrap_or_else(|e| panic!("Failed to open index in {:?}: {}", index_dir, e))
    }

    fn account_query(&self, account_id: u64) -> Box<TermQuery> {
        let account_term =
            Term::from_field_u64(SchemaTools::email_fields().f_account_id, account_id);
        Box::new(TermQuery::new(account_term, IndexRecordOption::Basic))
    }

    fn mailbox_query(&self, account_id: u64, mailbox_id: u64) -> Box<dyn Query> {
        let account_query = TermQuery::new(
            Term::from_field_u64(SchemaTools::email_fields().f_account_id, account_id),
            IndexRecordOption::Basic,
        );
        let mailbox_query = TermQuery::new(
            Term::from_field_u64(SchemaTools::email_fields().f_mailbox_id, mailbox_id),
            IndexRecordOption::Basic,
        );
        let boolean_query = BooleanQuery::new(vec![
            (Occur::Must, Box::new(account_query)),
            (Occur::Must, Box::new(mailbox_query)),
        ]);
        Box::new(boolean_query)
    }

    /// Return all Message-IDs stored in Tantivy for a given mailbox.
    /// Prefer `mailbox_contains_message_id` for existence checks on large
    /// mailboxes — this method loads everything into a HashSet.
    pub fn get_message_ids_for_mailbox(
        &self,
        account_id: u64,
        mailbox_id: u64,
    ) -> BichonResult<HashSet<String>> {
        let query = self.mailbox_query(account_id, mailbox_id);
        let fields = SchemaTools::email_fields();
        let searcher = self.create_searcher()?;

        let docs = searcher
            .search(&query, &DocSetCollector)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        let mut result = HashSet::new();
        for doc_address in docs {
            let doc = searcher
                .doc::<TantivyDocument>(doc_address)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

            if let Some(v) = doc.get_first(fields.f_message_id) {
                if let Some(s) = v.as_str() {
                    if !s.is_empty() {
                        result.insert(s.to_string());
                    }
                }
            }
        }
        Ok(result)
    }

    /// Check whether a specific Message-ID exists in a mailbox.
    /// Uses a TermQuery — O(1) per call, no allocation proportional to
    /// mailbox size. Suitable for large mailboxes where
    /// `get_message_ids_for_mailbox` would allocate too much memory.
    pub fn mailbox_contains_message_id(
        &self,
        account_id: u64,
        mailbox_id: u64,
        message_id: &str,
    ) -> BichonResult<bool> {
        let fields = SchemaTools::email_fields();
        let query = BooleanQuery::new(vec![
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_u64(fields.f_account_id, account_id),
                    IndexRecordOption::Basic,
                )),
            ),
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_u64(fields.f_mailbox_id, mailbox_id),
                    IndexRecordOption::Basic,
                )),
            ),
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(fields.f_message_id, message_id),
                    IndexRecordOption::Basic,
                )),
            ),
        ]);
        let searcher = self.create_searcher()?;
        let count = searcher
            .search(&query, &Count)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        Ok(count > 0)
    }

    fn envelope_query(&self, account_id: u64, eid: &str) -> Box<dyn Query> {
        let account_id_query = TermQuery::new(
            Term::from_field_u64(SchemaTools::email_fields().f_account_id, account_id),
            IndexRecordOption::Basic,
        );
        let envelope_id_query = TermQuery::new(
            Term::from_field_text(SchemaTools::email_fields().f_id, eid),
            IndexRecordOption::Basic,
        );
        let boolean_query = BooleanQuery::new(vec![
            (Occur::Must, Box::new(account_id_query)),
            (Occur::Must, Box::new(envelope_id_query)),
        ]);
        Box::new(boolean_query)
    }

    fn filter_query(
        &self,
        accounts: Option<HashSet<u64>>,
        filter: EmailSearchFilter,
    ) -> BichonResult<Box<dyn Query>> {
        let f = SchemaTools::email_fields();
        let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(authorized_ids) = accounts {
            if authorized_ids.is_empty() {
                let term = Term::from_field_u64(f.f_account_id, u64::MAX);
                subqueries.push((
                    Occur::Must,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
                ));
            } else {
                let mut account_must_queries = Vec::new();
                for id in authorized_ids {
                    let term = Term::from_field_u64(f.f_account_id, id);
                    account_must_queries.push((
                        Occur::Should,
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                    ));
                }
                subqueries.push((
                    Occur::Must,
                    Box::new(BooleanQuery::new(account_must_queries)),
                ));
            }
        }

        if let Some(ref text) = filter.text {
            let query_parser =
                QueryParser::for_index(&self.index, SchemaTools::email_default_fields());

            let query = query_parser
                .parse_query(text)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InvalidParameter))?;
            subqueries.push((Occur::Must, Box::new(query)));
        }

        if let Some(ref subject_val) = filter.subject {
            let query_parser = QueryParser::for_index(&self.index, vec![f.f_subject]);
            let q = query_parser
                .parse_query(subject_val)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InvalidParameter))?;
            subqueries.push((Occur::Must, q));
        }

        if let Some(ref body_val) = filter.body {
            let query_parser = QueryParser::for_index(&self.index, vec![f.f_body]);

            let q = query_parser
                .parse_query(body_val)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InvalidParameter))?;
            subqueries.push((Occur::Must, q));
        }

        if let Some(ref tags) = filter.tags {
            if !tags.is_empty() {
                let mut should_queries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

                for tag in tags {
                    let facet = Facet::from_text(tag).map_err(|e| {
                        raise_error!(format!("{:#?}", e), ErrorCode::InvalidParameter)
                    })?;

                    let term = Term::from_facet(f.f_tags, &facet);

                    should_queries.push((
                        Occur::Should,
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
                    ));
                }
                subqueries.push((Occur::Must, Box::new(BooleanQuery::new(should_queries))));
            }
        }

        for (field, opt_value) in [
            (f.f_from_text, &filter.from),
            (f.f_to_text, &filter.to),
            (f.f_cc_text, &filter.cc),
            (f.f_bcc_text, &filter.bcc),
        ] {
            if let Some(ref v) = opt_value {
                let query_parser = QueryParser::for_index(&self.index, vec![field]);
                let q = query_parser
                    .parse_query(v)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InvalidParameter))?;
                subqueries.push((Occur::Must, q));
            }
        }

        if let Some(has) = filter.has_attachment {
            let lower: Bound<Term>;
            let upper: Bound<Term>;

            if has {
                lower = Bound::Included(Term::from_field_u64(f.f_regular_attachment_count, 1));
                upper =
                    Bound::Included(Term::from_field_u64(f.f_regular_attachment_count, u64::MAX));
            } else {
                lower = Bound::Included(Term::from_field_u64(f.f_regular_attachment_count, 0));
                upper = Bound::Included(Term::from_field_u64(f.f_regular_attachment_count, 0));
            }

            subqueries.push((Occur::Must, Box::new(RangeQuery::new(lower, upper))));
        }

        if let Some(ref name) = filter.attachment_name {
            if name.contains('.') {
                let term = Term::from_field_text(f.f_attachment_name_exact, name);
                let exact_query = TermQuery::new(term, IndexRecordOption::Basic);

                let query_parser =
                    QueryParser::for_index(&self.index, vec![f.f_attachment_name_text]);
                let q: Box<dyn Query> = query_parser
                    .parse_query(name)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InvalidParameter))?;
                let query = BooleanQuery::new(vec![
                    (Occur::Should, Box::new(exact_query)),
                    (Occur::Should, Box::new(q)),
                ]);
                subqueries.push((Occur::Must, Box::new(query)));
            } else {
                let query_parser =
                    QueryParser::for_index(&self.index, vec![f.f_attachment_name_text]);
                let q = query_parser
                    .parse_query(name)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InvalidParameter))?;
                subqueries.push((Occur::Must, q));
            }
        }

        if let Some(ref id) = filter.id {
            let term = Term::from_field_text(f.f_id, id);
            let query = TermQuery::new(term, IndexRecordOption::Basic);
            subqueries.push((Occur::Must, Box::new(query)));
        }

        if let Some(ref extension) = filter.attachment_extension {
            let term = Term::from_field_text(f.f_attachment_ext, extension);
            let query = TermQuery::new(term, IndexRecordOption::Basic);
            subqueries.push((Occur::Must, Box::new(query)));
        }

        if let Some(ref category) = filter.attachment_category {
            let term = Term::from_field_text(f.f_attachment_category, category);
            let query = TermQuery::new(term, IndexRecordOption::Basic);
            subqueries.push((Occur::Must, Box::new(query)));
        }

        if let Some(ref content_type) = filter.attachment_content_type {
            let term = Term::from_field_text(f.f_attachment_content_type, content_type);
            let query = TermQuery::new(term, IndexRecordOption::Basic);
            subqueries.push((Occur::Must, Box::new(query)));
        }

        let start_bound = if let Some(from) = filter.since {
            Bound::Included(Term::from_field_i64(f.f_date, from))
        } else {
            Bound::Unbounded
        };

        let end_bound = if let Some(to) = filter.before {
            Bound::Included(Term::from_field_i64(f.f_date, to))
        } else {
            Bound::Unbounded
        };

        if start_bound != Bound::Unbounded || end_bound != Bound::Unbounded {
            let q = RangeQuery::new(start_bound, end_bound);
            subqueries.push((Occur::Must, Box::new(q)));
        }

        let start_bound = if let Some(from) = filter.internal_date_since {
            Bound::Included(Term::from_field_i64(f.f_internal_date, from))
        } else {
            Bound::Unbounded
        };

        let end_bound = if let Some(to) = filter.internal_date_before {
            Bound::Included(Term::from_field_i64(f.f_internal_date, to))
        } else {
            Bound::Unbounded
        };

        if start_bound != Bound::Unbounded || end_bound != Bound::Unbounded {
            let q = RangeQuery::new(start_bound, end_bound);
            subqueries.push((Occur::Must, Box::new(q)));
        }

        let start_bound = if let Some(from) = filter.ingest_since {
            Bound::Included(Term::from_field_i64(f.f_ingest_at, from))
        } else {
            Bound::Unbounded
        };

        let end_bound = if let Some(to) = filter.ingest_before {
            Bound::Included(Term::from_field_i64(f.f_ingest_at, to))
        } else {
            Bound::Unbounded
        };

        if start_bound != Bound::Unbounded || end_bound != Bound::Unbounded {
            let q = RangeQuery::new(start_bound, end_bound);
            subqueries.push((Occur::Must, Box::new(q)));
        }

        if let Some(account_ids) = filter.account_ids {
            let mut should_queries: Vec<(Occur, Box<dyn Query>)> = Vec::new();
            for id in account_ids {
                let term = Term::from_field_u64(f.f_account_id, id);
                should_queries.push((
                    Occur::Should,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
                ));
            }
            subqueries.push((Occur::Must, Box::new(BooleanQuery::new(should_queries))));
        }

        if let Some(mailbox_ids) = filter.mailbox_ids {
            let mut should_queries: Vec<(Occur, Box<dyn Query>)> = Vec::new();
            for id in mailbox_ids {
                let term = Term::from_field_u64(f.f_mailbox_id, id);
                should_queries.push((
                    Occur::Should,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
                ));
            }
            subqueries.push((Occur::Must, Box::new(BooleanQuery::new(should_queries))));
        }

        let start_bound = if let Some(from) = filter.min_size {
            Bound::Included(Term::from_field_u64(f.f_size, from))
        } else {
            Bound::Unbounded
        };

        let end_bound = if let Some(to) = filter.max_size {
            Bound::Included(Term::from_field_u64(f.f_size, to))
        } else {
            Bound::Unbounded
        };

        if start_bound != Bound::Unbounded || end_bound != Bound::Unbounded {
            let q = RangeQuery::new(start_bound, end_bound);
            subqueries.push((Occur::Must, Box::new(q)));
        }

        if let Some(ref msg_id) = filter.message_id {
            let term = Term::from_field_text(f.f_message_id, msg_id);
            subqueries.push((
                Occur::Must,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
            ));
        }

        if subqueries.is_empty() {
            return Ok(Box::new(AllQuery));
        }

        Ok(Box::new(BooleanQuery::new(subqueries)))
    }

    fn thread_query(&self, account_id: u64, thread_id: &str) -> Box<dyn Query> {
        let account_query = TermQuery::new(
            Term::from_field_u64(SchemaTools::email_fields().f_account_id, account_id),
            IndexRecordOption::Basic,
        );
        let thread_query = TermQuery::new(
            Term::from_field_text(SchemaTools::email_fields().f_thread_id, thread_id),
            IndexRecordOption::Basic,
        );
        let boolean_query = BooleanQuery::new(vec![
            (Occur::Must, Box::new(account_query)),
            (Occur::Must, Box::new(thread_query)),
        ]);
        Box::new(boolean_query)
    }

    pub fn get_envelope_by_id(
        &self,
        account_id: u64,
        envelope_id: &str,
    ) -> BichonResult<Option<EnvelopeWithAttachments>> {
        let searcher = self.create_searcher()?;
        let f = SchemaTools::email_fields();

        let query = BooleanQuery::new(vec![
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_u64(f.f_account_id, account_id),
                    IndexRecordOption::Basic,
                )),
            ),
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(f.f_id, envelope_id),
                    IndexRecordOption::Basic,
                )),
            ),
        ]);

        let docs: Vec<(f32, DocAddress)> = searcher
            .search(&query, &TopDocs::with_limit(1).order_by_score())
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        if let Some((_, doc_address)) = docs.first() {
            let doc: TantivyDocument = searcher
                .doc(*doc_address)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
            let envelope = EnvelopeWithAttachments::from_tantivy_doc(&doc)?;
            Ok(Some(envelope))
        } else {
            Ok(None)
        }
    }

    pub fn top_10_largest_emails(
        &self,
        accounts: &Option<HashSet<u64>>,
    ) -> BichonResult<Vec<LargestEmail>> {
        self.reader
            .reload()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        let searcher = self.reader.searcher();

        let query: Box<dyn Query> = match accounts {
            Some(ref ids) if !ids.is_empty() => {
                let mut subqueries = Vec::new();
                for &id in ids {
                    let term = Term::from_field_u64(SchemaTools::email_fields().f_account_id, id);
                    subqueries.push((
                        Occur::Should,
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                    ));
                }
                Box::new(BooleanQuery::new(subqueries))
            }
            Some(_) => Box::new(EmptyQuery),
            None => Box::new(AllQuery),
        };

        let mailbox_docs: Vec<(Option<u64>, DocAddress)> = searcher
            .search(
                &query,
                &TopDocs::with_limit(10).order_by_fast_field(F_SIZE, Order::Desc),
            )
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        let mut result = Vec::new();

        for (_, doc_address) in mailbox_docs {
            let doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
            let envelope = LargestEmail::from_tantivy_doc(&doc)?;
            result.push(envelope);
        }
        Ok(result)
    }

    pub fn total_emails(&self, accounts: &Option<HashSet<u64>>) -> BichonResult<u64> {
        let searcher = self.create_searcher()?;

        match accounts {
            Some(ref ids) if !ids.is_empty() => {
                let mut subqueries = Vec::new();
                for &id in ids {
                    let term = Term::from_field_u64(SchemaTools::email_fields().f_account_id, id);
                    subqueries.push((
                        Occur::Should,
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                    ));
                }
                let query = Box::new(BooleanQuery::new(subqueries)) as Box<dyn Query>;
                let count = searcher
                    .search(&query, &Count)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                Ok(count as u64)
            }
            Some(_) => Ok(0),
            None => Ok(searcher.num_docs()),
        }
    }

    /// Returns the number of emails indexed in Tantivy for a given mailbox.
    ///
    /// Used during incremental sync to detect an interrupted initial sync:
    /// if `local_count < remote.exists` and `highest_uid` is `None`, the
    /// mailbox was only partially downloaded and needs a full re-fetch.
    pub fn count_emails_in_mailbox(
        &self,
        account_id: u64,
        mailbox_id: u64,
    ) -> BichonResult<u64> {
        let searcher = self.create_searcher()?;
        let query = self.mailbox_query(account_id, mailbox_id);
        let count = searcher
            .search(query.as_ref(), &Count)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        Ok(count as u64)
    }

    pub fn get_max_uid(&self, account_id: u64, mailbox_id: u64) -> BichonResult<Option<u64>> {
        let searcher = self.create_searcher()?;

        let query = self.mailbox_query(account_id, mailbox_id);
        let agg_req: Aggregations = serde_json::from_value(json!({
            "max_uid": {
                "max": {
                    "field": F_UID
                }
            }
        }))
        .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        let collector = AggregationCollector::from_aggs(agg_req, Default::default());

        let agg_res = searcher
            .search(query.as_ref(), &collector)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        let result = Self::extract_max_uid(&agg_res);
        tracing::debug!(
            "[account {}][mailbox {}] get_max_uid = {:?} (num_docs in searcher = {})",
            account_id,
            mailbox_id,
            result,
            searcher.num_docs()
        );
        Ok(result)
    }

    pub fn get_account_stats(&self, account_id: u64) -> BichonResult<AccountStats> {
        let searcher = self.create_searcher()?;
        let query = self.account_query(account_id);

        let agg_req: Aggregations = serde_json::from_value(json!({
            "total_count": {
                "value_count": {
                    "field": F_ID
                }
            },
            "total_size": {
                "sum": {
                    "field": F_SIZE
                }
            }
        }))
        .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        let collector = AggregationCollector::from_aggs(agg_req, Default::default());
        let agg_res = searcher
            .search(query.as_ref(), &collector)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        let mut stats = AccountStats::default();

        stats.total_count = Self::extract_value_count(&agg_res, "total_count")?;

        let result = agg_res.0.get("total_size").ok_or_else(|| {
            raise_error!(
                "missing 'total_size' aggregation result".into(),
                ErrorCode::InternalError
            )
        })?;

        if let AggregationResult::MetricResult(MetricResult::Sum(v)) = result {
            stats.total_size = v.value.map(|v| v as u64).ok_or_else(|| {
                raise_error!(
                    "'total_size' sum metric has no value".into(),
                    ErrorCode::InternalError
                )
            })?;
        }
        Ok(stats)
    }

    fn extract_max_uid(agg_res: &AggregationResults) -> Option<u64> {
        agg_res.0.get("max_uid").and_then(|result| match result {
            AggregationResult::MetricResult(MetricResult::Max(max)) => {
                max.value.and_then(|value| {
                    (value >= 0.0 && value <= u64::MAX as f64).then(|| value.trunc() as u64)
                })
            }
            _ => None,
        })
    }

    pub async fn delete_account_envelopes(&self, account_id: u64) -> BichonResult<()> {
        let query = self.account_query(account_id);
        let (eml_content_hashes, attachments_content_hashes) =
            self.collect_content_hashes(query)?;

        let query = self.account_query(account_id);

        let mut writer = self.index_writer.lock().await;
        writer
            .delete_query(query)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        writer
            .commit()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        ATTACHMENT_MANAGER
            .delete_account_attachments(account_id)
            .await?;

        if !eml_content_hashes.is_empty() || !attachments_content_hashes.is_empty() {
            self.cleanup_unused_content(
                &mut writer,
                eml_content_hashes,
                attachments_content_hashes,
            )?;
        }

        DEDUP_CACHE.remove_by_account(account_id);
        Ok(())
    }

    pub async fn delete_mailbox_envelopes(
        &self,
        account_id: u64,
        mailbox_ids: Vec<u64>,
    ) -> BichonResult<()> {
        if mailbox_ids.is_empty() {
            return Ok(());
        }

        let mut eml_content_hashes: HashSet<String> = HashSet::new();
        let mut attachments_content_hashes: HashSet<String> = HashSet::new();

        for mailbox_id in &mailbox_ids {
            let query = self.mailbox_query(account_id, *mailbox_id);
            let (eml_hashes, attachment_hashes) = self.collect_content_hashes(query)?;
            eml_content_hashes.extend(eml_hashes);
            attachments_content_hashes.extend(attachment_hashes);
        }

        let mut queries: Vec<Box<dyn Query>> = Vec::with_capacity(mailbox_ids.len());
        for mailbox_id in &mailbox_ids {
            queries.push(self.mailbox_query(account_id, *mailbox_id));
        }
        let mut writer = self.index_writer.lock().await;
        for query in queries {
            writer
                .delete_query(query)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        }
        writer
            .commit()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        if !eml_content_hashes.is_empty() || !attachments_content_hashes.is_empty() {
            self.cleanup_unused_content(
                &mut writer,
                eml_content_hashes,
                attachments_content_hashes,
            )?;
        }

        for mailbox_id in mailbox_ids {
            DEDUP_CACHE.remove_by_mailbox(mailbox_id);
        }

        Ok(())
    }

    fn collect_content_hashes(
        &self,
        query: Box<dyn Query>,
    ) -> BichonResult<(HashSet<String>, HashSet<String>)> {
        let (eml_with_mailbox, attachments_content_hashes) =
            self.collect_content_hashes_with_mailbox(query)?;

        let eml_content_hashes = eml_with_mailbox
            .into_iter()
            .map(|(hash, _mailbox_id)| hash)
            .collect();

        Ok((eml_content_hashes, attachments_content_hashes))
    }

    fn collect_content_hashes_with_mailbox(
        &self,
        query: Box<dyn Query>,
    ) -> BichonResult<(HashSet<(String, u64)>, HashSet<String>)> {
        let mut eml_content_hashes = HashSet::new();
        let mut attachments_content_hashes = HashSet::new();

        let fields = SchemaTools::email_fields();
        let searcher = self.create_searcher()?;

        let docs = searcher
            .search(&query, &DocSetCollector)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        for doc_address in docs {
            let doc = searcher
                .doc::<TantivyDocument>(doc_address)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

            let mailbox_id = doc.get_first(fields.f_mailbox_id).and_then(|v| v.as_u64());

            // Extract content_hash
            if let Some(content_hash_value) = doc.get_first(fields.f_content_hash) {
                if let (Some(hash_str), Some(mailbox_id)) =
                    (content_hash_value.as_str(), mailbox_id)
                {
                    eml_content_hashes.insert((hash_str.to_string(), mailbox_id));
                }
            }

            // Extract attachment_content_hash
            let attachment_hash_values = doc.get_all(fields.f_attachment_content_hash);
            for hash_value in attachment_hash_values {
                if let Some(str) = hash_value.as_str() {
                    attachments_content_hashes.insert(str.to_string());
                }
            }
        }

        Ok((eml_content_hashes, attachments_content_hashes))
    }

    fn cleanup_unused_content(
        &self,
        writer: &mut IndexWriter,
        eml_content_hashes: HashSet<String>,
        attachments_content_hashes: HashSet<String>,
    ) -> BichonResult<()> {
        // Reference-count barrier: commit the writer and reload the reader so the
        // `Count` below is evaluated against a fully committed, freshly-reloaded
        // index state. Without this, an envelope that shares a content hash but
        // is still sitting uncommitted in the writer buffer (e.g. added by the
        // background ingest task before this delete acquired the writer lock)
        // would be invisible to the searcher, the count would read 0, and a
        // still-referenced blob would be deleted.
        fatal_commit(writer);
        let searcher = self.create_searcher()?;
        let fields = SchemaTools::email_fields();
        let mut eml: HashSet<String> = HashSet::new();
        for content_hash in eml_content_hashes {
            // Check if any other emails still reference this content hash
            let hash_term = Term::from_field_text(fields.f_content_hash, &content_hash);
            let hash_query = TermQuery::new(hash_term, IndexRecordOption::Basic);
            let count = searcher
                .search(&hash_query, &Count)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

            // If no references found, delete from KV store
            if count == 0 {
                eml.insert(content_hash);
            }
        }
        let mut attachments: HashSet<String> = HashSet::new();
        for content_hash in attachments_content_hashes {
            // Check if any other emails still reference this content hash
            let hash_term = Term::from_field_text(fields.f_attachment_content_hash, &content_hash);
            let hash_query = TermQuery::new(hash_term, IndexRecordOption::Basic);
            let count = searcher
                .search(&hash_query, &Count)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

            // If no references found, delete from KV store
            if count == 0 {
                attachments.insert(content_hash);
            }
        }

        BLOB_MANAGER.delete(&eml, &attachments)
    }

    pub async fn delete_envelopes_multi_account(
        &self,
        deletes: HashMap<u64, Vec<String>>,
    ) -> BichonResult<()> {
        if deletes.is_empty() {
            tracing::warn!("delete_envelopes_multi_account: deletes is empty, nothing to delete");
            return Ok(());
        }

        let mut eml_content_hash_triples: HashSet<(u64, u64, String)> = HashSet::new();
        let mut attachments_content_hashes: HashSet<String> = HashSet::new();

        for (account_id, envelope_ids) in &deletes {
            let unique_ids: HashSet<&String> = envelope_ids.iter().collect();
            if unique_ids.is_empty() {
                continue;
            }

            for eid in unique_ids {
                let query = self.envelope_query(*account_id, eid);
                let (eml_hashes_with_mailbox, attachment_hashes) =
                    self.collect_content_hashes_with_mailbox(query)?;

                eml_content_hash_triples.extend(
                    eml_hashes_with_mailbox
                        .into_iter()
                        .map(|(hash, mailbox_id)| (*account_id, mailbox_id, hash)),
                );
                attachments_content_hashes.extend(attachment_hashes);
            }
        }

        let mut writer = self.index_writer.lock().await;

        for (account_id, envelope_ids) in deletes {
            let unique_ids: HashSet<&String> = envelope_ids.iter().collect();
            if unique_ids.is_empty() {
                continue;
            }
            for eid in unique_ids {
                let query = self.envelope_query(account_id, eid);
                writer
                    .delete_query(query)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
            }
        }
        writer
            .commit()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        if !eml_content_hash_triples.is_empty() || !attachments_content_hashes.is_empty() {
            let eml_content_hashes: HashSet<String> = eml_content_hash_triples
                .iter()
                .map(|(_, _, hash)| hash.clone())
                .collect();

            self.cleanup_unused_content(
                &mut writer,
                eml_content_hashes,
                attachments_content_hashes,
            )?;
        }

        for (aid, mid, hash) in eml_content_hash_triples {
            DEDUP_CACHE.remove(aid, mid, &hash);
        }

        Ok(())
    }

    fn collect_facets_recursive(
        query: &dyn Query,
        searcher: &Searcher,
        parent_facet: &str,
        all_facets: &mut Vec<TagCount>,
    ) -> BichonResult<()> {
        let mut facet_collector = FacetCollector::for_field(F_TAGS);
        facet_collector.add_facet(parent_facet);

        let facet_counts = searcher
            .search(query, &facet_collector)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        for (facet, count) in facet_counts.get(parent_facet) {
            all_facets.push(TagCount {
                tag: facet.to_string(),
                count,
            });
            Self::collect_facets_recursive(query, searcher, &facet.to_string(), all_facets)?;
        }

        Ok(())
    }

    pub fn get_all_tags(&self, accounts: Option<HashSet<u64>>) -> BichonResult<Vec<TagCount>> {
        let searcher = self.reader.searcher();

        let query: Box<dyn Query> = match accounts {
            Some(ref ids) if !ids.is_empty() => {
                let mut subquotes = Vec::new();
                for &id in ids {
                    let term = Term::from_field_u64(SchemaTools::email_fields().f_account_id, id);
                    subquotes.push((
                        Occur::Should,
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                    ));
                }
                Box::new(BooleanQuery::new(subquotes))
            }
            Some(_) => Box::new(EmptyQuery),
            None => Box::new(AllQuery),
        };

        let mut all_facets = Vec::new();
        Self::collect_facets_recursive(&query, &searcher, "/", &mut all_facets)?;
        Ok(all_facets)
    }

    pub fn get_all_contacts(
        &self,
        accounts: Option<HashSet<u64>>,
    ) -> BichonResult<HashSet<String>> {
        let searcher = self.create_searcher()?;

        let query: Box<dyn Query> = match accounts {
            Some(ref ids) if !ids.is_empty() => {
                let mut subquotes = Vec::new();
                for &id in ids {
                    let term = Term::from_field_u64(SchemaTools::email_fields().f_account_id, id);
                    subquotes.push((
                        Occur::Should,
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                    ));
                }
                Box::new(BooleanQuery::new(subquotes))
            }
            Some(_) => Box::new(EmptyQuery),
            None => Box::new(AllQuery),
        };

        let mut contacts_set: HashSet<String> = HashSet::new();

        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(1_000_000).order_by_score())
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
            let contacts = extract_contacts(&doc)?;
            for value in contacts {
                contacts_set.insert(value);
            }
        }
        Ok(contacts_set)
    }

    pub async fn update_envelope_tags(&self, request: TagsRequest) -> BichonResult<()> {
        if request.updates.is_empty() {
            return Ok(());
        }

        let mut writer = self.index_writer.lock().await;
        let searcher = self.create_searcher()?;
        let f = SchemaTools::email_fields();

        for update in request.updates {
            let query = self.envelope_query(update.account_id, &update.envelope_id);

            let docs: Vec<DocAddress> = searcher
                .search(&query, &DocSetCollector)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?
                .into_iter()
                .collect();

            for doc_address in docs {
                let existing_doc: TantivyDocument = searcher
                    .doc(doc_address)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

                let mut envelope = EnvelopeWithAttachments::from_tantivy_doc(&existing_doc)?;

                match update.action {
                    TagAction::Add => {
                        for tag in &update.tags {
                            if !envelope.tags.contains(tag) {
                                envelope.tags.push(tag.clone());
                            }
                        }
                    }
                    TagAction::Remove => {
                        envelope.tags.retain(|t| !update.tags.contains(t));
                    }
                    TagAction::Set => {
                        envelope.tags = update.tags.clone();
                    }
                }

                writer
                    .delete_query(self.envelope_query(update.account_id, &update.envelope_id))
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

                let new_doc = envelope.to_tantivy_doc()?;
                writer
                    .add_document(new_doc)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
            }
        }

        writer
            .commit()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        Ok(())
    }

    pub fn search(
        &self,
        accounts: Option<HashSet<u64>>,
        filter: EmailSearchFilter,
        page: u32,
        page_size: u32,
    ) -> BichonResult<DataPage<Envelope>> {
        let searcher = self.create_searcher()?;
        let query = self.filter_query(accounts, filter.clone())?;

        let sort_by = filter.sort_by.unwrap_or(SortBy::Date);

        let offset = (page * page_size) as usize;
        let limit = page_size as usize;

        let (total, doc_addresses) = match sort_by {
            SortBy::Date => {
                let collector = TopDocs::with_limit(limit + offset)
                    .order_by_fast_field::<i64>(F_DATE, Order::Desc);
                let top_docs = searcher
                    .search(&query, &collector)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let total = searcher
                    .search(&query, &Count)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let addresses: Vec<DocAddress> = top_docs
                    .into_iter()
                    .skip(offset)
                    .map(|(_, addr)| addr)
                    .collect();
                (total, addresses)
            }
            SortBy::InternalDate => {
                let collector = TopDocs::with_limit(limit + offset)
                    .order_by_fast_field::<i64>(F_INTERNAL_DATE, Order::Desc);
                let top_docs = searcher
                    .search(&query, &collector)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let total = searcher
                    .search(&query, &Count)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let addresses: Vec<DocAddress> = top_docs
                    .into_iter()
                    .skip(offset)
                    .map(|(_, addr)| addr)
                    .collect();
                (total, addresses)
            }
            SortBy::IngestAt => {
                let collector = TopDocs::with_limit(limit + offset)
                    .order_by_fast_field::<i64>(F_INGEST_AT, Order::Desc);
                let top_docs = searcher
                    .search(&query, &collector)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let total = searcher
                    .search(&query, &Count)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let addresses: Vec<DocAddress> = top_docs
                    .into_iter()
                    .skip(offset)
                    .map(|(_, addr)| addr)
                    .collect();
                (total, addresses)
            }
            SortBy::From => {
                let collector = TopDocs::with_limit(limit + offset)
                    .order_by_fast_field::<u64>(F_FROM, Order::Asc);
                let top_docs = searcher
                    .search(&query, &collector)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let total = searcher
                    .search(&query, &Count)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let addresses: Vec<DocAddress> = top_docs
                    .into_iter()
                    .skip(offset)
                    .map(|(_, addr)| addr)
                    .collect();
                (total, addresses)
            }
            SortBy::Size => {
                let collector = TopDocs::with_limit(limit + offset)
                    .order_by_fast_field::<u64>(F_SIZE, Order::Desc);
                let top_docs = searcher
                    .search(&query, &collector)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let total = searcher
                    .search(&query, &Count)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let addresses: Vec<DocAddress> = top_docs
                    .into_iter()
                    .skip(offset)
                    .map(|(_, addr)| addr)
                    .collect();
                (total, addresses)
            }
            SortBy::Relevance => {
                let collector = TopDocs::with_limit(limit + offset).order_by_score();
                let top_docs = searcher
                    .search(&query, &collector)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let total = searcher
                    .search(&query, &Count)
                    .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
                let addresses: Vec<DocAddress> = top_docs
                    .into_iter()
                    .skip(offset)
                    .map(|(_, addr)| addr)
                    .collect();
                (total, addresses)
            }
        };

        let mut envelopes: Vec<Envelope> = Vec::with_capacity(doc_addresses.len());
        for doc_address in doc_addresses {
            let doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
            let envelope = Envelope::from_tantivy_doc(&doc)?;
            envelopes.push(envelope);
        }

        Ok(DataPage {
            data: envelopes,
            total: total as u64,
            page,
            page_size,
        })
    }

    pub fn get_thread(
        &self,
        account_id: u64,
        thread_id: &str,
    ) -> BichonResult<Vec<EnvelopeWithAttachments>> {
        let searcher = self.create_searcher()?;
        let query = self.thread_query(account_id, thread_id);

        let top_docs = searcher
            .search(
                &query,
                &TopDocs::with_limit(1000).order_by_fast_field::<i64>(F_DATE, Order::Asc),
            )
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        let mut envelopes = Vec::new();
        for (_, doc_address) in top_docs {
            let doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
            let envelope = EnvelopeWithAttachments::from_tantivy_doc(&doc)?;
            envelopes.push(envelope);
        }

        Ok(envelopes)
    }

    pub fn create_searcher(&self) -> BichonResult<Searcher> {
        self.reader
            .reload()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        Ok(self.reader.searcher())
    }

    pub fn ingest_envelope(
        &self,
        account_id: u64,
        mailbox_id: u64,
        uid: u64,
        envelope: &Envelope,
        raw_eml: &[u8],
    ) -> BichonResult<()> {
        let fields = SchemaTools::email_fields();
        let mut doc = TantivyDocument::new();

        doc.add_u64(fields.f_account_id, account_id);
        doc.add_u64(fields.f_mailbox_id, mailbox_id);
        doc.add_u64(fields.f_uid, uid);

        doc.add_text(fields.f_id, &envelope.id);
        doc.add_text(fields.f_content_hash, &envelope.content_hash);
        doc.add_text(fields.f_message_id, &envelope.message_id);
        doc.add_text(fields.f_thread_id, &envelope.thread_id);

        if let Some(date) = envelope.date {
            doc.add_i64(fields.f_date, date);
        }
        if let Some(internal_date) = envelope.internal_date {
            doc.add_i64(fields.f_internal_date, internal_date);
        }
        doc.add_i64(fields.f_ingest_at, utc_now!());

        doc.add_u64(fields.f_size, envelope.size);

        if let Some(ref subject) = envelope.subject {
            doc.add_text(fields.f_subject, subject);
        }

        for tag in &envelope.tags {
            if let Ok(facet) = Facet::from_text(tag) {
                doc.add_facet(fields.f_tags, facet);
            }
        }

        let parsed = MessageParser::default().parse(raw_eml);
        if let Some(ref msg) = parsed {
            let body_text = msg
                .text_bodies()
                .map(|p| p.text_contents().unwrap_or_default().to_string())
                .collect::<Vec<_>>()
                .join("\n");

            let html_text: String = msg
                .html_bodies()
                .map(|p| {
                    let raw = p.text_contents().unwrap_or_default();
                    extract_text(raw)
                })
                .collect::<Vec<_>>()
                .join("\n");

            let combined = format!("{}\n{}", body_text, html_text);
            doc.add_text(fields.f_body, &combined);

            let from_contacts = extract_contacts_from_header(msg, "from");
            let to_contacts = extract_contacts_from_header(msg, "to");
            let cc_contacts = extract_contacts_from_header(msg, "cc");
            let bcc_contacts = extract_contacts_from_header(msg, "bcc");

            doc.add_text(fields.f_from_text, &from_contacts.join(" "));
            doc.add_text(fields.f_to_text, &to_contacts.join(" "));
            doc.add_text(fields.f_cc_text, &cc_contacts.join(" "));
            doc.add_text(fields.f_bcc_text, &bcc_contacts.join(" "));

            let from_hash = if let Some(addr) = msg.from().and_then(|f| f.first()) {
                let normalized = addr
                    .address()
                    .map(|a| a.to_lowercase())
                    .unwrap_or_default();
                stable_hash_u64(&normalized)
            } else {
                0
            };
            doc.add_u64(fields.f_from, from_hash);

            let mut attachment_count: u64 = 0;
            for attachment in msg.attachments() {
                let content_type = attachment
                    .content_type()
                    .map(|ct| format!("{}/{}", ct.c_type, ct.c_subtype.as_deref().unwrap_or("*")))
                    .unwrap_or_default();

                let filename = attachment
                    .attachment_name()
                    .unwrap_or_default()
                    .to_lowercase();

                let extension = filename
                    .rfind('.')
                    .map(|i| &filename[i + 1..])
                    .unwrap_or_default()
                    .to_string();

                let category = categorize_attachment(&content_type, &extension);

                let is_inline = attachment
                    .content_disposition()
                    .map(|d| d.c_type.eq_ignore_ascii_case("inline"))
                    .unwrap_or(false);

                if !is_inline {
                    attachment_count += 1;
                }

                doc.add_text(fields.f_attachment_name_exact, &filename);
                doc.add_text(fields.f_attachment_name_text, &filename);
                doc.add_text(fields.f_attachment_ext, &extension);
                doc.add_text(fields.f_attachment_content_type, &content_type);
                doc.add_text(fields.f_attachment_category, &category);
            }

            doc.add_u64(fields.f_regular_attachment_count, attachment_count);
        }

        let writer = self.index_writer.blocking_lock();
        writer
            .add_document(doc)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        Ok(())
    }

    pub async fn ingest_envelope_async(
        &self,
        account_id: u64,
        mailbox_id: u64,
        uid: u64,
        envelope: &Envelope,
        raw_eml: &[u8],
    ) -> BichonResult<()> {
        let fields = SchemaTools::email_fields();
        let mut doc = TantivyDocument::new();

        doc.add_u64(fields.f_account_id, account_id);
        doc.add_u64(fields.f_mailbox_id, mailbox_id);
        doc.add_u64(fields.f_uid, uid);

        doc.add_text(fields.f_id, &envelope.id);
        doc.add_text(fields.f_content_hash, &envelope.content_hash);
        doc.add_text(fields.f_message_id, &envelope.message_id);
        doc.add_text(fields.f_thread_id, &envelope.thread_id);

        if let Some(date) = envelope.date {
            doc.add_i64(fields.f_date, date);
        }
        if let Some(internal_date) = envelope.internal_date {
            doc.add_i64(fields.f_internal_date, internal_date);
        }
        doc.add_i64(fields.f_ingest_at, utc_now!());

        doc.add_u64(fields.f_size, envelope.size);

        if let Some(ref subject) = envelope.subject {
            doc.add_text(fields.f_subject, subject);
        }

        for tag in &envelope.tags {
            if let Ok(facet) = Facet::from_text(tag) {
                doc.add_facet(fields.f_tags, facet);
            }
        }

        let parsed = MessageParser::default().parse(raw_eml);
        if let Some(ref msg) = parsed {
            let body_text = msg
                .text_bodies()
                .map(|p| p.text_contents().unwrap_or_default().to_string())
                .collect::<Vec<_>>()
                .join("\n");

            let html_text: String = msg
                .html_bodies()
                .map(|p| {
                    let raw = p.text_contents().unwrap_or_default();
                    extract_text(raw)
                })
                .collect::<Vec<_>>()
                .join("\n");

            let combined = format!("{}\n{}", body_text, html_text);
            doc.add_text(fields.f_body, &combined);

            let from_contacts = extract_contacts_from_header(msg, "from");
            let to_contacts = extract_contacts_from_header(msg, "to");
            let cc_contacts = extract_contacts_from_header(msg, "cc");
            let bcc_contacts = extract_contacts_from_header(msg, "bcc");

            doc.add_text(fields.f_from_text, &from_contacts.join(" "));
            doc.add_text(fields.f_to_text, &to_contacts.join(" "));
            doc.add_text(fields.f_cc_text, &cc_contacts.join(" "));
            doc.add_text(fields.f_bcc_text, &bcc_contacts.join(" "));

            let from_hash = if let Some(addr) = msg.from().and_then(|f| f.first()) {
                let normalized = addr
                    .address()
                    .map(|a| a.to_lowercase())
                    .unwrap_or_default();
                stable_hash_u64(&normalized)
            } else {
                0
            };
            doc.add_u64(fields.f_from, from_hash);

            let mut attachment_count: u64 = 0;
            for attachment in msg.attachments() {
                let content_type = attachment
                    .content_type()
                    .map(|ct| format!("{}/{}", ct.c_type, ct.c_subtype.as_deref().unwrap_or("*")))
                    .unwrap_or_default();

                let filename = attachment
                    .attachment_name()
                    .unwrap_or_default()
                    .to_lowercase();

                let extension = filename
                    .rfind('.')
                    .map(|i| &filename[i + 1..])
                    .unwrap_or_default()
                    .to_string();

                let category = categorize_attachment(&content_type, &extension);

                let is_inline = attachment
                    .content_disposition()
                    .map(|d| d.c_type.eq_ignore_ascii_case("inline"))
                    .unwrap_or(false);

                if !is_inline {
                    attachment_count += 1;
                }

                doc.add_text(fields.f_attachment_name_exact, &filename);
                doc.add_text(fields.f_attachment_name_text, &filename);
                doc.add_text(fields.f_attachment_ext, &extension);
                doc.add_text(fields.f_attachment_content_type, &content_type);
                doc.add_text(fields.f_attachment_category, &category);

                let content_hash = envelope.content_hash.clone();
                doc.add_text(fields.f_attachment_content_hash, &content_hash);
            }

            doc.add_u64(fields.f_regular_attachment_count, attachment_count);
        }

        self.queue(doc).await;

        Ok(())
    }

    pub fn get_dashboard_stats(
        &self,
        accounts: &Option<HashSet<u64>>,
    ) -> BichonResult<DashboardStats> {
        let searcher = self.create_searcher()?;

        let query: Box<dyn Query> = match accounts {
            Some(ref ids) if !ids.is_empty() => {
                let mut subqueries = Vec::new();
                for &id in ids {
                    let term = Term::from_field_u64(SchemaTools::email_fields().f_account_id, id);
                    subqueries.push((
                        Occur::Should,
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                    ));
                }
                Box::new(BooleanQuery::new(subqueries))
            }
            Some(_) => Box::new(EmptyQuery),
            None => Box::new(AllQuery),
        };

        let now = Utc::now().timestamp();
        let day_secs: i64 = 86_400;
        let week_ago = now - 7 * day_secs;
        let month_ago = now - 30 * day_secs;
        let year_ago = now - 365 * day_secs;

        let f = SchemaTools::email_fields();
        let agg_req: Aggregations = serde_json::from_value(json!({
            "total_count": { "value_count": { "field": F_ID } },
            "total_size":  { "sum":         { "field": F_SIZE } },
            "max_size":    { "max":         { "field": F_SIZE } },
            "last_7d":  {
                "filter": { "range": { F_INGEST_AT: { "gte": week_ago,  "lte": now } } },
                "aggs": { "count": { "value_count": { "field": F_ID } } }
            },
            "last_30d": {
                "filter": { "range": { F_INGEST_AT: { "gte": month_ago, "lte": now } } },
                "aggs": { "count": { "value_count": { "field": F_ID } } }
            },
            "last_365d": {
                "filter": { "range": { F_INGEST_AT: { "gte": year_ago,  "lte": now } } },
                "aggs": { "count": { "value_count": { "field": F_ID } } }
            },
            "by_account": {
                "terms": { "field": F_ACCOUNT_ID, "size": 100 },
                "aggs": {
                    "count": { "value_count": { "field": F_ID   } },
                    "size":  { "sum":         { "field": F_SIZE } }
                }
            },
            "by_date_histogram": {
                "date_histogram": {
                    "field": F_DATE,
                    "fixed_interval": "86400000",
                    "min_doc_count": 1
                }
            }
        }))
        .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        let collector = AggregationCollector::from_aggs(agg_req, Default::default());
        let agg_res = searcher
            .search(&query, &collector)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        let total_count = Self::extract_value_count(&agg_res, "total_count")?;

        let total_size = {
            let r = agg_res.0.get("total_size").ok_or_else(|| {
                raise_error!("missing 'total_size'".into(), ErrorCode::InternalError)
            })?;
            match r {
                AggregationResult::MetricResult(MetricResult::Sum(v)) => {
                    v.value.map(|v| v as u64).unwrap_or(0)
                }
                _ => 0,
            }
        };

        let max_size = {
            let r = agg_res.0.get("max_size").ok_or_else(|| {
                raise_error!("missing 'max_size'".into(), ErrorCode::InternalError)
            })?;
            match r {
                AggregationResult::MetricResult(MetricResult::Max(v)) => {
                    v.value.map(|v| v as u64).unwrap_or(0)
                }
                _ => 0,
            }
        };

        let last_7d = Self::extract_filter_count(&agg_res, "last_7d")?;
        let last_30d = Self::extract_filter_count(&agg_res, "last_30d")?;
        let last_365d = Self::extract_filter_count(&agg_res, "last_365d")?;

        let by_account = Self::extract_terms_groups(&agg_res, "by_account")?;
        let by_date_histogram = Self::extract_date_histogram(&agg_res, "by_date_histogram")?;

        Ok(DashboardStats {
            total_count,
            total_size,
            max_size,
            last_7d,
            last_30d,
            last_365d,
            by_account,
            by_date_histogram,
        })
    }

    fn extract_value_count(agg_res: &AggregationResults, key: &str) -> BichonResult<u64> {
        let result = agg_res.0.get(key).ok_or_else(|| {
            raise_error!(
                format!("missing '{}' aggregation result", key),
                ErrorCode::InternalError
            )
        })?;
        match result {
            AggregationResult::MetricResult(MetricResult::Count(v)) => {
                Ok(v.value.map(|v| v as u64).unwrap_or(0))
            }
            _ => Err(raise_error!(
                format!("unexpected result type for '{}'", key),
                ErrorCode::InternalError
            )),
        }
    }

    fn extract_filter_count(agg_res: &AggregationResults, key: &str) -> BichonResult<u64> {
        let result = agg_res.0.get(key).ok_or_else(|| {
            raise_error!(
                format!("missing '{}' aggregation result", key),
                ErrorCode::InternalError
            )
        })?;
        match result {
            AggregationResult::BucketResult(BucketResult::Filter(filter)) => {
                let sub = filter.buckets.0.get("count").ok_or_else(|| {
                    raise_error!(
                        format!("missing 'count' sub-aggregation in '{}'", key),
                        ErrorCode::InternalError
                    )
                })?;
                match sub {
                    AggregationResult::MetricResult(MetricResult::Count(v)) => {
                        Ok(v.value.map(|v| v as u64).unwrap_or(0))
                    }
                    _ => Err(raise_error!(
                        format!("unexpected sub-result type for '{}' count", key),
                        ErrorCode::InternalError
                    )),
                }
            }
            _ => Err(raise_error!(
                format!("unexpected result type for '{}'", key),
                ErrorCode::InternalError
            )),
        }
    }

    fn extract_terms_groups(agg_res: &AggregationResults, key: &str) -> BichonResult<Vec<Group>> {
        let result = agg_res.0.get(key).ok_or_else(|| {
            raise_error!(
                format!("missing '{}' aggregation result", key),
                ErrorCode::InternalError
            )
        })?;
        match result {
            AggregationResult::BucketResult(BucketResult::Terms(terms)) => {
                let mut groups = Vec::new();
                if let BucketEntries::U64(entries) = &terms.buckets {
                    for bucket in entries {
                        let count_val = bucket.sub_aggregations.0.get("count").and_then(|r| {
                            if let AggregationResult::MetricResult(MetricResult::Count(v)) = r {
                                v.value.map(|v| v as u64)
                            } else {
                                None
                            }
                        }).unwrap_or(0);

                        let size_val = bucket.sub_aggregations.0.get("size").and_then(|r| {
                            if let AggregationResult::MetricResult(MetricResult::Sum(v)) = r {
                                v.value.map(|v| v as u64)
                            } else {
                                None
                            }
                        }).unwrap_or(0);

                        groups.push(Group {
                            key: bucket.key.to_string(),
                            count: count_val,
                            size: size_val,
                        });
                    }
                }
                Ok(groups)
            }
            _ => Err(raise_error!(
                format!("unexpected result type for '{}'", key),
                ErrorCode::InternalError
            )),
        }
    }

    fn extract_date_histogram(
        agg_res: &AggregationResults,
        key: &str,
    ) -> BichonResult<Vec<TimeBucket>> {
        let result = agg_res.0.get(key).ok_or_else(|| {
            raise_error!(
                format!("missing '{}' aggregation result", key),
                ErrorCode::InternalError
            )
        })?;
        match result {
            AggregationResult::BucketResult(BucketResult::Histogram(hist)) => {
                let mut buckets = Vec::new();
                for bucket in &hist.buckets {
                    let ts = match &bucket.key {
                        Key::F64(v) => (*v / 1000.0) as i64,
                        Key::Str(s) => s.parse::<i64>().unwrap_or(0),
                    };
                    buckets.push(TimeBucket {
                        timestamp: ts,
                        count: bucket.doc_count,
                    });
                }
                Ok(buckets)
            }
            _ => Err(raise_error!(
                format!("unexpected result type for '{}'", key),
                ErrorCode::InternalError
            )),
        }
    }

    pub fn migrate_account(&self, model: &AccountModel) -> BichonResult<()> {
        warn!("Migrating account {} in Tantivy index", model.id);
        let searcher = self.create_searcher()?;
        let query = self.account_query(model.id);
        let f = SchemaTools::email_fields();

        let docs: Vec<DocAddress> = searcher
            .search(&query, &DocSetCollector)
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?
            .into_iter()
            .collect();

        let writer = self.index_writer.blocking_lock();

        for doc_address in docs {
            let existing_doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

            let mut envelope = EnvelopeWithAttachments::from_tantivy_doc(&existing_doc)?;

            for (old_tag, new_tag) in &model.tag_migrations {
                if let Some(pos) = envelope.tags.iter().position(|t| t == old_tag) {
                    envelope.tags[pos] = new_tag.clone();
                }
            }

            writer
                .delete_query(self.envelope_query(model.id, &envelope.id))
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

            let new_doc = envelope.to_tantivy_doc()?;
            writer
                .add_document(new_doc)
                .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        }

        let mut writer = self.index_writer.blocking_lock();
        writer
            .commit()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

        Ok(())
    }
}

fn extract_contacts_from_header(msg: &mail_parser::Message, header: &str) -> Vec<String> {
    msg.header(header)
        .and_then(|h| {
            if let mail_parser::HeaderValue::AddressList(addrs) = &h.value {
                Some(
                    addrs
                        .iter()
                        .flat_map(|a| {
                            let mut parts = Vec::new();
                            if let Some(name) = &a.name {
                                parts.push(name.to_string());
                            }
                            if let Some(addr) = &a.address {
                                parts.push(addr.to_string());
                            }
                            parts
                        })
                        .collect::<Vec<_>>(),
                )
            } else if let mail_parser::HeaderValue::Address(addr) = &h.value {
                let mut parts = Vec::new();
                if let Some(name) = &addr.name {
                    parts.push(name.to_string());
                }
                if let Some(a) = &addr.address {
                    parts.push(a.to_string());
                }
                Some(parts)
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn stable_hash_u64(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

fn categorize_attachment(content_type: &str, extension: &str) -> String {
    match content_type.split('/').next().unwrap_or("") {
        "image" => "image".to_string(),
        "video" => "video".to_string(),
        "audio" => "audio".to_string(),
        "text" => "text".to_string(),
        _ => match extension {
            "pdf" => "pdf".to_string(),
            "doc" | "docx" | "odt" | "rtf" => "document".to_string(),
            "xls" | "xlsx" | "ods" | "csv" => "spreadsheet".to_string(),
            "ppt" | "pptx" | "odp" => "presentation".to_string(),
            "zip" | "tar" | "gz" | "rar" | "7z" => "archive".to_string(),
            "exe" | "dmg" | "pkg" | "deb" | "rpm" => "executable".to_string(),
            _ => "other".to_string(),
        },
    }
}
