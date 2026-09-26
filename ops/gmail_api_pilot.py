#!/usr/bin/env python3
"""Read-only Gmail API check of captured IDs and handoff history boundary.

The OAuth token and output profile stay in owner-only files. No message bodies,
IDs, account names, URLs with credentials, or bearer tokens are printed.
"""

import argparse
import json
import os
from pathlib import Path
import sqlite3
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from gmail_adopt import mapping_digest, source_email


GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
BASE_URL = "https://gmail.googleapis.com/gmail/v1/users/me"


def gmail_get(access_token, path):
    request = Request(BASE_URL + path, headers={
        "Authorization": "Bearer " + access_token,
        "Accept": "application/json",
    })
    try:
        with urlopen(request, timeout=20) as response:
            return json.load(response)
    except HTTPError as error:
        raise ValueError(f"Gmail API returned HTTP {error.code}") from None
    except URLError:
        raise ValueError("Gmail API transport failed") from None


def pilot(archive, ledger, token, *, limit, request=gmail_get):
    if GMAIL_READONLY_SCOPE not in token.get("scopes", []):
        raise ValueError("token lacks recorded Gmail read-only grant")
    access_token = token.get("access_token", "")
    if not access_token:
        raise ValueError("token has no access token")
    binding = ledger.execute("SELECT archive_uid, source_id FROM migration").fetchall()
    if len(binding) != 1:
        raise ValueError("capture ledger has no unique archive binding")
    archive_uid, source_id = binding[0]
    actual_uid = archive.execute(
        "SELECT value FROM archive_metadata WHERE key = 'archive_uid'").fetchone()
    if actual_uid is None or actual_uid[0] != archive_uid:
        raise ValueError("capture ledger belongs to another archive")
    source = archive.execute(
        "SELECT source_type, identifier FROM sources WHERE id = ?", (source_id,)
    ).fetchone()
    if source is None or source[0] != "imap":
        raise ValueError("original IMAP source is unavailable")
    account = source_email(source[1])
    profile = request(access_token, "/profile")
    email = profile.get("emailAddress", "")
    history_id = str(profile.get("historyId", ""))
    if not email or email.lower() != account.lower() or not history_id.isdecimal():
        raise ValueError("Gmail API profile does not match archived account")
    candidates = ledger.execute("""
        SELECT message_id, source_message_id, gmail_id, gmail_thread_id
        FROM capture WHERE status = 'mapped'
        ORDER BY message_id DESC LIMIT ?
    """, (limit,)).fetchall()
    if len(candidates) != limit:
        raise ValueError("pilot requires the requested number of mapped rows")
    raw_count = 0
    attachment_messages = 0
    for message_id, old_id, gmail_id, thread_id in candidates:
        actual = archive.execute("""
            SELECT source_message_id FROM messages
            WHERE id = ? AND source_id = ?
        """, (message_id, source_id)).fetchone()
        if actual is None or actual[0] != old_id or not gmail_id or not thread_id:
            raise ValueError("archived identity changed or mapping incomplete")
        remote = request(access_token, "/messages/" + quote(gmail_id) + "?format=minimal")
        if remote.get("id") != gmail_id or remote.get("threadId") != thread_id:
            raise ValueError("Gmail API identity disagrees with IMAP mapping")
        raw_count += archive.execute(
            "SELECT EXISTS(SELECT 1 FROM message_raw WHERE message_id = ?)",
            (message_id,)).fetchone()[0]
        attachment_messages += archive.execute(
            "SELECT EXISTS(SELECT 1 FROM attachments WHERE message_id = ?)",
            (message_id,)).fetchone()[0]
    return {"email": email, "history_id": history_id,
            "archive_uid": archive_uid, "source_id": source_id,
            "verified_count": len(candidates),
            "mapping_digest": mapping_digest(candidates)}, {
        "api_ids_and_threads_verified": len(candidates),
        "existing_raw_messages": raw_count,
        "existing_attachment_messages": attachment_messages,
        "archive_rows_mutated": 0,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--token", type=Path, required=True)
    parser.add_argument("--profile-output", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if not 1 <= args.limit <= 100:
        parser.error("pilot limit must be 1..100")
    if args.profile_output.exists():
        parser.error("profile handoff already exists; refusing to overwrite boundary")
    for path in (args.archive, args.ledger, args.token):
        if not path.is_file() or path.is_symlink():
            parser.error("inputs must be existing regular files")
    if args.token.stat().st_mode & 0o077:
        parser.error("OAuth token must be owner-only")
    token = json.loads(args.token.read_text())
    archive = sqlite3.connect(f"file:{args.archive.resolve()}?mode=ro", uri=True)
    ledger = sqlite3.connect(f"file:{args.ledger.resolve()}?mode=ro", uri=True)
    try:
        profile, result = pilot(archive, ledger, token, limit=args.limit)
    finally:
        archive.close()
        ledger.close()
    fd = os.open(args.profile_output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(profile, output)
        output.write("\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
