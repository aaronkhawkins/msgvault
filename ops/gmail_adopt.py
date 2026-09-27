#!/usr/bin/env python3
"""Adopt exact Gmail IDs in an existing IMAP archive without replacing rows.

The default pilot applies the real transaction to a disposable SQLite backup.
An authoritative handoff requires --apply and a complete capture ledger. Run it
only while the MsgVault daemon and its scheduled writer are stopped.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from urllib.parse import unquote, urlsplit


HEX_ID = re.compile(r"[0-9a-f]+\Z")
DECIMAL_ID = re.compile(r"[1-9][0-9]*\Z")


def mapping_digest(rows):
    """Bind a pilot to exact archived/provider IDs without storing them twice."""
    digest = hashlib.sha256()
    for message_id, old_id, gmail_id, thread_id in rows:
        digest.update(json.dumps([message_id, old_id, gmail_id, thread_id],
                                 separators=(",", ":")).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def source_email(identifier):
    parsed = urlsplit(identifier)
    if parsed.scheme != "imaps" or parsed.hostname != "imap.gmail.com" or parsed.port != 993:
        raise ValueError("source is not authenticated Gmail IMAP")
    email = unquote(parsed.username or "")
    if not email or "@" not in email:
        raise ValueError("source account identity is invalid")
    return email


def validate_retained_manifest(manifest, archive_uid, source_id):
    if manifest is None:
        return {}
    if not isinstance(manifest, dict) or \
            manifest.get("archive_uid") != archive_uid or \
            manifest.get("source_id") != source_id or \
            not isinstance(manifest.get("retained_rows"), list):
        raise ValueError("retained manifest belongs to another archive/source")
    retained = {}
    for row in manifest["retained_rows"]:
        if not isinstance(row, dict) or set(row) != {
                "message_id", "source_message_id", "gmail_id", "reason"} or \
                type(row["message_id"]) is not int or row["message_id"] < 1 or \
                not isinstance(row["source_message_id"], str) or \
                row["reason"] not in ("provider_absent", "duplicate_provider_id"):
            raise ValueError("invalid retained manifest row")
        if row["message_id"] in retained:
            raise ValueError("duplicate retained manifest row")
        retained[row["message_id"]] = row
    return retained


def adopt(archive, ledger, profile, *, complete, retained_manifest=None):
    """Perform one validated transaction; caller owns both SQLite handles.

    A partial adoption is only suitable for a disposable pilot: unadopted rows
    would otherwise be eligible for duplicate imports by native Gmail sync.
    """
    email = profile.get("email", "")
    history_id = str(profile.get("history_id", ""))
    if not DECIMAL_ID.fullmatch(history_id):
        raise ValueError("Gmail profile history ID is required")
    bindings = ledger.execute(
        "SELECT archive_uid, source_id FROM migration").fetchall()
    if len(bindings) != 1:
        raise ValueError("capture ledger has no unique archive binding")
    archive_uid, source_id = bindings[0]
    retained = validate_retained_manifest(retained_manifest, archive_uid, source_id)
    actual_uid = archive.execute(
        "SELECT value FROM archive_metadata WHERE key = 'archive_uid'").fetchone()
    if actual_uid is None or actual_uid[0] != archive_uid:
        raise ValueError("capture ledger belongs to another archive")
    verified_count = profile.get("verified_count")
    if profile.get("archive_uid") != archive_uid or \
            profile.get("source_id") != source_id or \
            not isinstance(verified_count, int) or verified_count < 1:
        raise ValueError("Gmail API pilot is not bound to this archive/source")
    pilot_rows = ledger.execute("""
        SELECT message_id, source_message_id, gmail_id, gmail_thread_id
        FROM capture WHERE status = 'mapped'
        ORDER BY message_id DESC LIMIT ?
    """, (verified_count,)).fetchall()
    if len(pilot_rows) != verified_count or \
            profile.get("mapping_digest") != mapping_digest(pilot_rows):
        raise ValueError("verified Gmail API pilot mappings changed")
    row = archive.execute(
        "SELECT source_type, identifier FROM sources WHERE id = ?", (source_id,)
    ).fetchone()
    if row is None or row[0] != "imap" or source_email(row[1]).lower() != email.lower():
        raise ValueError("source identity or authenticated Gmail profile differs")
    if archive.execute(
        "SELECT 1 FROM sources WHERE source_type = 'gmail' AND lower(identifier) = lower(?)",
        (email,)).fetchone():
        raise ValueError("native Gmail source already exists; refusing duplicate")

    captures = ledger.execute("""
        SELECT message_id, source_message_id, gmail_id, gmail_thread_id, status
        FROM capture ORDER BY message_id
    """).fetchall()
    if not captures:
        raise ValueError("capture ledger is empty")
    if complete:
        count = archive.execute(
            "SELECT COUNT(*) FROM messages WHERE source_id = ?", (source_id,)
        ).fetchone()[0]
        if len(captures) != count:
            raise ValueError("capture ledger does not cover the whole source")

    canonical_by_gmail = {}
    canonical_threads = {}
    retained_duplicates = []
    mapped = 0
    for message_id, old_id, gmail_id, thread_id, status in captures:
        actual = archive.execute("""
            SELECT source_message_id, conversation_id FROM messages
            WHERE id = ? AND source_id = ?
        """, (message_id, source_id)).fetchone()
        if actual is None or actual[0] != old_id:
            raise ValueError("capture row no longer matches archived identity")
        selected = retained.get(message_id)
        if selected is not None:
            if selected["source_message_id"] != old_id or \
                    selected["gmail_id"] != gmail_id:
                raise ValueError("retained manifest no longer matches capture")
            if selected["reason"] == "provider_absent" and \
                    status == "missing" and gmail_id is None and thread_id is None:
                continue
            if selected["reason"] == "duplicate_provider_id" and \
                    status == "mapped" and gmail_id and thread_id and \
                    HEX_ID.fullmatch(gmail_id) and HEX_ID.fullmatch(thread_id):
                retained_duplicates.append((gmail_id, thread_id))
                continue
            raise ValueError("retained row lacks the required capture evidence")
        # A UID FETCH miss is not proof of provider absence. It requires an
        # explicit, owner-only retained manifest after separate verification.
        if status != "mapped" or not gmail_id or not thread_id or \
                not HEX_ID.fullmatch(gmail_id) or not HEX_ID.fullmatch(thread_id):
            raise ValueError("capture contains an unresolved or invalid mapping")
        if gmail_id in canonical_by_gmail:
            raise ValueError("two archived rows map to one Gmail message")
        canonical_by_gmail[gmail_id] = (message_id, thread_id)
        canonical_threads[thread_id] = min(
            canonical_threads.get(thread_id, actual[1]), actual[1])
        mapped += 1
    captured_ids = {row[0] for row in captures}
    if any(message_id not in captured_ids for message_id in retained):
        raise ValueError("retained manifest contains an unknown capture row")
    if any(canonical_by_gmail.get(gmail_id, (None, None))[1] != thread_id
           for gmail_id, thread_id in retained_duplicates):
        raise ValueError("retained duplicate has no matching canonical Gmail row")

    # Lock and recheck current row identity inside the transaction. The daemon
    # must be stopped; SQLite will reject a concurrent writer rather than let
    # a partial source conversion leak into the live archive.
    archive.execute("BEGIN IMMEDIATE")
    try:
        if complete and archive.execute(
            "SELECT COUNT(*) FROM messages WHERE source_id = ?", (source_id,)
        ).fetchone()[0] != len(captures):
            raise ValueError("archive changed after coverage check")
        archive.execute("""
            CREATE TABLE IF NOT EXISTS gmail_thread_adoption (
                source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                gmail_thread_id TEXT NOT NULL,
                conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
                PRIMARY KEY (source_id, gmail_thread_id)
            )
        """)
        archive.execute("""
            CREATE TABLE IF NOT EXISTS gmail_archive_only_adoption (
                message_id INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
                source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE
            )
        """)
        for message_id, old_id, gmail_id, _thread_id, status in captures:
            if message_id in retained:
                archive.execute("""
                    INSERT INTO gmail_archive_only_adoption (message_id, source_id)
                    VALUES (?, ?)
                """, (message_id, source_id))
                continue
            result = archive.execute("""
                UPDATE messages SET source_message_id = ?
                WHERE id = ? AND source_id = ? AND source_message_id = ?
            """, (gmail_id, message_id, source_id, old_id))
            if result.rowcount != 1:
                raise ValueError("archive changed during adoption")
        for thread_id, conversation_id in canonical_threads.items():
            archive.execute("""
                INSERT INTO gmail_thread_adoption
                    (source_id, gmail_thread_id, conversation_id)
                VALUES (?, ?, ?)
            """, (source_id, thread_id, conversation_id))
        result = archive.execute("""
            UPDATE sources SET source_type = 'gmail', identifier = ?,
                sync_cursor = NULL
            WHERE id = ? AND source_type = 'imap'
        """, (email, source_id))
        if result.rowcount != 1:
            raise ValueError("source changed during adoption")
        archive.commit()
    except Exception:
        archive.rollback()
        raise
    return {"mapped": mapped, "retained": len(retained),
            "threads_routed": len(canonical_threads),
            "source_id_preserved": True, "cursor_pending_full_reconciliation": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True,
                      help="owner-only JSON containing Gmail profile email and history_id")
    parser.add_argument("--retained-manifest", type=Path,
                        help="owner-only explicit provider-absent and duplicate rows")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--pilot-copy", type=Path,
                      help="disposable output database; subset adoption allowed")
    mode.add_argument("--apply", action="store_true",
                      help="authoritative handoff; requires complete capture")
    args = parser.parse_args()
    for protected in (args.archive, args.ledger, args.profile):
        if not protected.is_file() or protected.is_symlink():
            parser.error("archive, ledger, and profile must be existing regular files")
    if args.profile.stat().st_mode & 0o077:
        parser.error("Gmail profile handoff must be owner-only")
    profile = json.loads(args.profile.read_text())
    retained_manifest = None
    if args.retained_manifest:
        if not args.retained_manifest.is_file() or args.retained_manifest.is_symlink() or \
                args.retained_manifest.stat().st_mode & 0o077:
            parser.error("retained manifest must be an owner-only regular file")
        retained_manifest = json.loads(args.retained_manifest.read_text())
    ledger = sqlite3.connect(f"file:{args.ledger.resolve()}?mode=ro", uri=True)
    try:
        if args.pilot_copy:
            if args.pilot_copy.exists():
                parser.error("pilot output already exists")
            source = sqlite3.connect(f"file:{args.archive.resolve()}?mode=ro", uri=True)
            old_umask = os.umask(0o077)
            try:
                pilot = sqlite3.connect(args.pilot_copy)
            finally:
                os.umask(old_umask)
            try:
                source.backup(pilot)
                result = adopt(pilot, ledger, profile, complete=False,
                               retained_manifest=retained_manifest)
            finally:
                source.close()
                pilot.close()
        else:
            archive = sqlite3.connect(args.archive)
            try:
                result = adopt(archive, ledger, profile, complete=True,
                               retained_manifest=retained_manifest)
            finally:
                archive.close()
        print(json.dumps(result))
    finally:
        ledger.close()


if __name__ == "__main__":
    main()
