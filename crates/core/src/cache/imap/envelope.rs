use crate::{
    raise_error,
    {
        account::migration::AccountModel,
        cache::{
            imap::mailbox::MailBox,
            trash::{TRASH_ENVELOPE_MANAGER, TRASH_MAIL_MANAGER},
        },
        error::{code::ErrorCode, BichonResult},
        store::tantivy::envelope::ENVELOPE_MANAGER,
    },
};
use std::collections::HashSet;
use tantivy::{
    collector::Count,
    query::{BooleanQuery, Occur, Query, TermQuery},
    schema::{IndexRecordOption, Term},
    Index,
};

pub fn get_max_uid(account_id: u64, mailbox_id: u64) -> BichonResult<Option<u64>> {
    ENVELOPE_MANAGER.get_max_uid(account_id, mailbox_id)
}

pub fn count_emails_in_mailbox(account_id: u64, mailbox_id: u64) -> BichonResult<u64> {
    ENVELOPE_MANAGER.count_emails_in_mailbox(account_id, mailbox_id)
}

pub fn get_uid_set(
    account: &AccountModel,
    mailbox: &MailBox,
    uids: &[u32],
) -> BichonResult<HashSet<u32>> {
    ENVELOPE_MANAGER.get_uid_set(account, mailbox, uids)
}

pub fn get_uid_set_not_in_trash(
    account: &AccountModel,
    mailbox: &MailBox,
    uids: &[u32],
) -> BichonResult<HashSet<u32>> {
    let mut result = HashSet::new();
    let uid_set = ENVELOPE_MANAGER.get_uid_set(account, mailbox, uids)?;

    if let Some(trash_mailbox) = MailBox::find_trash_mailbox(account.id)? {
        let trash_uid_set =
            TRASH_ENVELOPE_MANAGER.get_uid_set(account, &trash_mailbox, uids)?;
        result.extend(uid_set.difference(&trash_uid_set).copied());
    } else {
        result = uid_set;
    }

    Ok(result)
}

pub fn search_by_order_id(
    email: &str,
    account_id: u64,
    mailbox: &MailBox,
) -> BichonResult<bool> {
    let searcher = ENVELOPE_MANAGER.create_searcher()?;
    let query = ENVELOPE_MANAGER.message_id_query(account_id, mailbox.id, email);
    let count = searcher
        .search(query.as_ref(), &Count)
        .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

    Ok(count > 0)
}

pub fn delete_by_ids(ids: Vec<&str>) -> BichonResult<()> {
    if ids.is_empty() {
        return Ok(());
    }

    ENVELOPE_MANAGER.batch_delete_envelopes(&ids)?;
    TRASH_MAIL_MANAGER.batch_delete_emails(&ids)?;

    Ok(())
}

pub fn delete_by_account(account_id: u64) -> BichonResult<()> {
    let searcher = ENVELOPE_MANAGER.create_searcher()?;
    let index = searcher.index();
    let f = crate::store::tantivy::schema::SchemaTools::email_fields();

    let term = Term::from_field_u64(f.f_account_id, account_id);
    let query = TermQuery::new(term, IndexRecordOption::Basic);

    let reader = index.reader().map_err(|e| {
        raise_error!(
            format!("Failed to create index reader: {:#?}", e),
            ErrorCode::InternalError,
        )
    })?;

    let searcher = reader.searcher();

    let count = searcher
        .search(&query, &Count)
        .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

    if count == 0 {
        return Ok(());
    }

    let writer = index
        .writer(50_000_000)
        .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

    writer
        .delete_query(Box::new(query))
        .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

    writer
        .commit()
        .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tantivy::{schema::SchemaTools, utils::EuroTokenizer};
    use tantivy::{
        collector::{Count, TopDocs},
        doc,
        Index,
    };

    #[test]
    fn test_delete_by_account() {
        let f = SchemaTools::email_fields();
        let index = Index::create_in_ram(SchemaTools::email_schema());
        index.tokenizers().register("euro", EuroTokenizer::new());

        let mut writer = index
            .writer_with_num_threads(1, 15_000_000)
            .unwrap();

        // Add documents for two accounts
        writer
            .add_document(doc!(f.f_account_id => 1u64, f.f_mailbox_id => 1u64, f.f_uid => 1u64))
            .unwrap();
        writer
            .add_document(doc!(f.f_account_id => 1u64, f.f_mailbox_id => 1u64, f.f_uid => 2u64))
            .unwrap();
        writer
            .add_document(doc!(f.f_account_id => 2u64, f.f_mailbox_id => 1u64, f.f_uid => 3u64))
            .unwrap();

        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();

        let account1_query: Box<dyn Query> = {
            let account_query = TermQuery::new(
                Term::from_field_u64(f.f_account_id, 1),
                IndexRecordOption::Basic,
            );
            Box::new(account_query)
        };

        let account2_query: Box<dyn Query> = {
            let account_query = TermQuery::new(
                Term::from_field_u64(f.f_account_id, 2),
                IndexRecordOption::Basic,
            );
            Box::new(account_query)
        };

        let account1_count = searcher
            .search(account1_query.as_ref(), &Count)
            .unwrap();
        let account2_count = searcher
            .search(account2_query.as_ref(), &Count)
            .unwrap();

        assert_eq!(account1_count, 2);
        assert_eq!(account2_count, 1);

        // Delete account 1 documents
        let mut writer = index
            .writer(50_000_000)
            .unwrap();

        let delete_query: Box<dyn Query> = {
            let account_query = TermQuery::new(
                Term::from_field_u64(f.f_account_id, 1),
                IndexRecordOption::Basic,
            );
            Box::new(account_query)
        };

        writer.delete_query(delete_query).unwrap();
        writer.commit().unwrap();

        reader.reload().unwrap();
        let searcher = reader.searcher();

        let account1_count_after = searcher
            .search(account1_query.as_ref(), &Count)
            .unwrap();
        let account2_count_after = searcher
            .search(account2_query.as_ref(), &Count)
            .unwrap();

        assert_eq!(account1_count_after, 0);
        assert_eq!(account2_count_after, 1);
    }

    #[test]
    fn count_emails_in_mailbox_counts_docs_correctly() {
        let f = SchemaTools::email_fields();
        let index = Index::create_in_ram(SchemaTools::email_schema());
        index.tokenizers().register("euro", EuroTokenizer::new());

        {
            let mut writer = index
                .writer_with_num_threads(1, 15_000_000)
                .expect("writer");

            // mailbox 10: 2 docs
            for uid in [1u64, 2u64] {
                let mut doc = tantivy::Document::new();
                doc.add_u64(f.f_account_id, 1);
                doc.add_u64(f.f_mailbox_id, 10);
                doc.add_u64(f.f_uid, uid);
                doc.add_text(f.f_id, format!("id-{}", uid));
                doc.add_text(f.f_content_hash, format!("hash-{}", uid));
                writer.add_document(doc).unwrap();
            }

            // mailbox 20: 1 doc
            let mut doc3 = tantivy::Document::new();
            doc3.add_u64(f.f_account_id, 1);
            doc3.add_u64(f.f_mailbox_id, 20);
            doc3.add_u64(f.f_uid, 3);
            doc3.add_text(f.f_id, "id-3");
            doc3.add_text(f.f_content_hash, "hash-3");
            writer.add_document(doc3).unwrap();

            writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();

        // mailbox 10
        let query_10: Box<dyn Query> = {
            let account_query = TermQuery::new(
                Term::from_field_u64(f.f_account_id, 1),
                IndexRecordOption::Basic,
            );
            let mailbox_query = TermQuery::new(
                Term::from_field_u64(f.f_mailbox_id, 10),
                IndexRecordOption::Basic,
            );
            Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(account_query)),
                (Occur::Must, Box::new(mailbox_query)),
            ]))
        };

        let count_10 = searcher
            .search(query_10.as_ref(), &Count)
            .expect("count search");
        assert_eq!(count_10, 2);

        // mailbox 20
        let query_20: Box<dyn Query> = {
            let account_query = TermQuery::new(
                Term::from_field_u64(f.f_account_id, 1),
                IndexRecordOption::Basic,
            );
            let mailbox_query = TermQuery::new(
                Term::from_field_u64(f.f_mailbox_id, 20),
                IndexRecordOption::Basic,
            );
            Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(account_query)),
                (Occur::Must, Box::new(mailbox_query)),
            ]))
        };

        let count_20 = searcher
            .search(query_20.as_ref(), &Count)
            .expect("count search");
        assert_eq!(count_20, 1);
    }
}
