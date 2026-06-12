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

/// Resets the sync state of a single mailbox so that the next sync
/// cycle performs a **full fetch** instead of an incremental one.
///
/// Concretely this clears:
/// - `highest_uid`  → triggers `fetch_and_save_full_mailbox` on next run
/// - `uid_next`     → stale, will be refreshed from IMAP EXAMINE
/// - `exists`       → stale count, will be refreshed from IMAP EXAMINE
///
/// The function does **not** delete any locally stored messages; it only
/// resets the watermark used to decide where to resume downloading.
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

    // Clear all sync-state fields so the next run starts from scratch.
    mailbox.highest_uid = None;
    mailbox.uid_next = None;
    mailbox.exists = 0;

    MailBox::batch_upsert(&[mailbox])?;
    Ok(())
}
