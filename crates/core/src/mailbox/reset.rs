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

use crate::{
    cache::imap::mailbox::MailBox,
    error::{code::ErrorCode, BichonResult},
    raise_error,
};

/// Schedules a forced full sync for a single mailbox.
///
/// Sets `force_full_sync = true` so that `perform_incremental_sync` bypasses
/// both `highest_uid` and the Tantivy fallback on the next sync cycle,
/// triggering `fetch_and_save_full_mailbox` unconditionally.
///
/// Also clears `highest_uid`, `uid_next`, and `exists` so the watermark is
/// fully reset in case the full sync is interrupted mid-way and the caller
/// inspects these fields directly.
///
/// Does **not** delete any locally stored messages — deduplication is handled
/// by the IMAP executor layer (`uid_batch_retrieve_emails` / `batch_retrieve_emails`).
pub async fn reset_mailbox_sync_impl(account_id: u64, mailbox_id: u64) -> BichonResult<()> {
    let mut mailbox = MailBox::find_mailbox(account_id, mailbox_id)?
        .ok_or_else(|| {
            raise_error!(
                format!(
                    "mailbox {} not found for account {}",
                    mailbox_id, account_id
                ),
                ErrorCode::NotFound
            )
        })?;

    mailbox.force_full_sync = true;
    mailbox.highest_uid = None;
    mailbox.uid_next = None;
    mailbox.exists = 0;

    MailBox::batch_upsert(&[mailbox])?;
    Ok(())
}
