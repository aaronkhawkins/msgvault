#!/usr/bin/env python3
"""Capture exact Gmail IDs for an existing IMAP archive into a private sidecar.

This reads the MsgVault archive and Gmail IMAP only. It never changes messages,
source type, sync cursors, attachments, or vector state. The sidecar is a
resumable input to a separately reviewed in-place cutover.
"""

import argparse
import imaplib
import json
import os
from pathlib import Path
import re
import sqlite3
import ssl
import tomllib
from urllib.parse import unquote, urlsplit


UID_RE = re.compile(rb"\bUID (\d+)\b")
GMAIL_ID_RE = re.compile(rb"\bX-GM-MSGID (\d+)\b")
GMAIL_THREAD_RE = re.compile(rb"\bX-GM-THRID (\d+)\b")
FETCH_SIZE = 25


def split_source_id(value):
    mailbox, separator, uid_text = value.rpartition("|")
    if not separator or not mailbox or not uid_text.isdecimal():
        return None
    uid = int(uid_text)
    return (mailbox, uid) if uid > 0 else None


def parse_fetch_response(records):
    found = {}
    for record in records:
        if not isinstance(record, bytes):
            continue
        uid = UID_RE.search(record)
        gmail_id = GMAIL_ID_RE.search(record)
        if uid and gmail_id:
            gmail_thread = GMAIL_THREAD_RE.search(record)
            found[int(uid.group(1))] = (
                format(int(gmail_id.group(1)), "x"),
                format(int(gmail_thread.group(1)), "x") if gmail_thread else None)
    return found


def open_ledger(path, archive_uid, source_id):
    if path.is_symlink():
        raise ValueError("migration ledger must not be a symlink")
    old_umask = os.umask(0o077)
    try:
        ledger = sqlite3.connect(path)
    finally:
        os.umask(old_umask)
    if path.stat().st_mode & 0o077:
        ledger.close()
        raise ValueError("migration ledger must be owner-only")
    ledger.execute("PRAGMA foreign_keys = ON")
    ledger.executescript("""
        CREATE TABLE IF NOT EXISTS migration (
          archive_uid TEXT NOT NULL,
          source_id INTEGER NOT NULL,
          next_before_id INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS capture (
          message_id INTEGER PRIMARY KEY,
          source_message_id TEXT NOT NULL,
          mailbox TEXT,
          uidvalidity INTEGER,
          gmail_id TEXT,
          gmail_thread_id TEXT,
          status TEXT NOT NULL CHECK (
            status IN ('mapped', 'missing', 'epoch_mismatch', 'no_epoch',
                       'invalid_source_id', 'select_error', 'fetch_error'))
        );
        CREATE INDEX IF NOT EXISTS capture_gmail_id ON capture(gmail_id);
    """)
    if "gmail_thread_id" not in {
            row[1] for row in ledger.execute("PRAGMA table_info(capture)")
    }:
        ledger.execute("ALTER TABLE capture ADD COLUMN gmail_thread_id TEXT")
    existing = ledger.execute(
        "SELECT archive_uid, source_id, next_before_id FROM migration"
    ).fetchall()
    if existing and (len(existing) != 1 or existing[0][:2] != (archive_uid, source_id)):
        ledger.close()
        raise ValueError("migration ledger belongs to a different archive/source")
    if not existing:
        ledger.execute("INSERT INTO migration VALUES (?, ?, ?)",
                       (archive_uid, source_id, 9223372036854775807))
        ledger.commit()
    return ledger


def record(ledger, message_id, source_message_id, mailbox, epoch,
           gmail_id, gmail_thread_id, status):
    with ledger:
        ledger.execute("""
            INSERT INTO capture
              (message_id, source_message_id, mailbox, uidvalidity,
               gmail_id, gmail_thread_id, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
              source_message_id = excluded.source_message_id,
              mailbox = excluded.mailbox,
              uidvalidity = excluded.uidvalidity,
              gmail_id = excluded.gmail_id,
              gmail_thread_id = excluded.gmail_thread_id,
              status = excluded.status
        """, (message_id, source_message_id, mailbox, epoch,
              gmail_id, gmail_thread_id, status))
        ledger.execute("UPDATE migration SET next_before_id = MIN(next_before_id, ?)",
                       (message_id,))


def capture(archive, ledger, config, token, limit, refresh_thread=False):
    source = archive.execute(
        "SELECT id, identifier FROM sources WHERE source_type = 'imap'"
    ).fetchall()
    if len(source) != 1:
        raise ValueError("expected exactly one IMAP source in this archive")
    source_id, identifier = source[0]
    archive_uid = archive.execute(
        "SELECT value FROM archive_metadata WHERE key = 'archive_uid'"
    ).fetchone()[0]
    ledger = open_ledger(ledger, archive_uid, source_id)
    try:
        before = ledger.execute("SELECT next_before_id FROM migration").fetchone()[0]
        if refresh_thread:
            candidates = ledger.execute("""
                SELECT message_id, source_message_id FROM capture
                WHERE gmail_thread_id IS NULL AND status = 'mapped'
                ORDER BY message_id DESC LIMIT ?
            """, (limit,)).fetchall()
        else:
            candidates = archive.execute("""
                SELECT id, source_message_id FROM messages
                WHERE source_id = ? AND id < ? ORDER BY id DESC LIMIT ?
            """, (source_id, before, limit)).fetchall()
        saved_epochs = dict(archive.execute(
            "SELECT mailbox, uidvalidity FROM imap_folder_state WHERE source_id = ?",
            (source_id,)))
        accounts = config.get("accounts") or []
        if len(accounts) != 1:
            raise ValueError("expected exactly one configured IMAP account")
        account = accounts[0]
        endpoint = urlsplit(identifier)
        if endpoint.scheme != "imaps" or endpoint.hostname != "imap.gmail.com" or \
                endpoint.port != 993 or unquote(endpoint.username or "") != account.get("email"):
            raise ValueError("capture requires authenticated TLS to imap.gmail.com")
        conn = imaplib.IMAP4_SSL("imap.gmail.com", 993,
                                 ssl_context=ssl.create_default_context(), timeout=20)
        try:
            conn.login(account["email"], token["password"])
            if b"X-GM-EXT-1" not in conn.capability()[1][0].upper():
                raise ValueError("provider lacks X-GM-MSGID support")
            outcomes = {}
            grouped = {}
            for message_id, source_message_id in candidates:
                parsed = split_source_id(source_message_id)
                if parsed is None:
                    outcomes[message_id] = (None, None, None, None, "invalid_source_id")
                else:
                    grouped.setdefault(parsed[0], []).append(
                        (message_id, source_message_id, parsed[1]))
            for mailbox, rows in grouped.items():
                epoch = saved_epochs.get(mailbox)
                if epoch is None:
                    for message_id, _, _ in rows:
                        outcomes[message_id] = (mailbox, None, None, None, "no_epoch")
                    continue
                escaped = mailbox.replace("\\", "\\\\").replace('"', '\\"')
                try:
                    status, _ = conn.select('"' + escaped + '"', readonly=True)
                    if status != "OK":
                        raise ValueError("select failed")
                    live_epoch = int(conn.response("UIDVALIDITY")[1][0])
                    if live_epoch != epoch:
                        for message_id, _, _ in rows:
                            outcomes[message_id] = (mailbox, epoch, None, None, "epoch_mismatch")
                        continue
                    for start in range(0, len(rows), FETCH_SIZE):
                        chunk = rows[start:start + FETCH_SIZE]
                        status, records = conn.uid(
                            "fetch", ",".join(str(row[2]) for row in chunk),
                            "(UID X-GM-MSGID X-GM-THRID)")
                        if status != "OK":
                            for message_id, _, _ in chunk:
                                outcomes[message_id] = (mailbox, epoch, None, None, "fetch_error")
                            continue
                        found = parse_fetch_response(records)
                        for message_id, _, uid in chunk:
                            gmail_id, gmail_thread_id = found.get(uid, (None, None))
                            outcomes[message_id] = (
                                mailbox, epoch, gmail_id, gmail_thread_id,
                                "mapped" if gmail_id else "missing")
                except (imaplib.IMAP4.error, OSError, ValueError):
                    for message_id, _, _ in rows:
                        outcomes.setdefault(message_id, (mailbox, epoch, None, None, "select_error"))
            for message_id, source_message_id in candidates:
                mailbox, epoch, gmail_id, gmail_thread_id, status = outcomes[message_id]
                record(ledger, message_id, source_message_id,
                       mailbox, epoch, gmail_id, gmail_thread_id, status)
            counts = dict(ledger.execute(
                "SELECT status, COUNT(*) FROM capture GROUP BY status"))
            duplicates = ledger.execute("""
                SELECT COUNT(*) FROM (
                  SELECT gmail_id FROM capture WHERE gmail_id IS NOT NULL
                  GROUP BY gmail_id HAVING COUNT(*) > 1)
            """).fetchone()[0]
            print(json.dumps({"processed_this_run": len(candidates),
                              "recorded_total": sum(counts.values()),
                              "status_counts": counts,
                              "duplicate_gmail_ids": duplicates}))
        finally:
            try:
                conn.logout()
            except OSError:
                pass
    finally:
        ledger.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--refresh-thread", action="store_true",
                        help="backfill thread IDs for already mapped rows")
    args = parser.parse_args()
    if not 1 <= args.limit <= 1000:
        parser.error("limit must be between 1 and 1000")
    root = args.home.expanduser().resolve()
    config = tomllib.loads((root / "config.toml").read_text())
    token_files = list((root / "tokens").glob("*.json"))
    if len(token_files) != 1:
        raise ValueError("expected exactly one protected IMAP token")
    token = json.loads(token_files[0].read_text())
    archive = sqlite3.connect(f"file:{root / 'msgvault.db'}?mode=ro", uri=True)
    try:
        capture(archive, args.ledger.expanduser(), config, token,
                args.limit, args.refresh_thread)
    finally:
        archive.close()


if __name__ == "__main__":
    main()
