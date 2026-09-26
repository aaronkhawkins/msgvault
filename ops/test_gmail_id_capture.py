import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "gmail_id_capture", Path(__file__).with_name("gmail_id_capture.py"))
capture_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture_module)


class FakeIMAP:
    def __init__(self, *_args, **_kwargs):
        self.mailbox = None

    def login(self, _user, _password):
        return "OK", []

    def capability(self):
        return "OK", [b"IMAP4rev1 X-GM-EXT-1"]

    def select(self, mailbox, readonly=False):
        assert readonly
        self.mailbox = mailbox
        return "OK", [b"3"]

    def response(self, code):
        assert code == "UIDVALIDITY"
        return "UIDVALIDITY", [b"45"]

    def uid(self, operation, numbers, fields):
        assert operation == "fetch"
        assert fields == "(UID X-GM-MSGID X-GM-THRID)"
        return "OK", [
            f"1 (UID {uid} X-GM-MSGID {1000 + int(uid)} X-GM-THRID 2000)".encode()
            for uid in numbers.split(",") if uid != "2"
        ]

    def logout(self):
        return "BYE", []


class CaptureTest(unittest.TestCase):
    def test_resumes_after_first_batch_and_retains_missing_record(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = sqlite3.connect(Path(directory) / "archive.db")
            archive.executescript("""
                CREATE TABLE sources (id INTEGER PRIMARY KEY, source_type TEXT, identifier TEXT);
                CREATE TABLE messages (id INTEGER PRIMARY KEY, source_id INTEGER, source_message_id TEXT);
                CREATE TABLE archive_metadata (key TEXT, value TEXT);
                CREATE TABLE imap_folder_state (source_id INTEGER, mailbox TEXT, uidvalidity INTEGER);
                INSERT INTO sources VALUES
                  (1, 'imap', 'imaps://synthetic%40example.test@imap.gmail.com:993');
                INSERT INTO archive_metadata VALUES ('archive_uid', 'synthetic-archive');
                INSERT INTO imap_folder_state VALUES (1, '[Gmail]/All Mail', 45);
                INSERT INTO messages VALUES
                  (1, 1, '[Gmail]/All Mail|1'),
                  (2, 1, '[Gmail]/All Mail|2'),
                  (3, 1, '[Gmail]/All Mail|3');
            """)
            ledger_path = Path(directory) / "mapping.db"
            config = {"accounts": [{"email": "synthetic@example.test"}]}
            with patch.object(capture_module.imaplib, "IMAP4_SSL", FakeIMAP):
                capture_module.capture(archive, ledger_path, config, {"password": "synthetic"}, 2)
                capture_module.capture(archive, ledger_path, config, {"password": "synthetic"}, 2)

            ledger = sqlite3.connect(ledger_path)
            self.assertEqual(
                [(3, "mapped", "3eb"), (2, "missing", None), (1, "mapped", "3e9")],
                ledger.execute("""
                    SELECT message_id, status, gmail_id FROM capture ORDER BY message_id DESC
                """).fetchall())
            self.assertEqual(1, ledger.execute(
                "SELECT next_before_id FROM migration").fetchone()[0])
            self.assertEqual(2, ledger.execute(
                "SELECT COUNT(*) FROM capture WHERE gmail_thread_id = '7d0'"
            ).fetchone()[0])
            self.assertEqual(0o600, ledger_path.stat().st_mode & 0o777)
            self.assertEqual(3, archive.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
            self.assertEqual("imap", archive.execute(
                "SELECT source_type FROM sources").fetchone()[0])
            ledger.close()
            archive.close()


if __name__ == "__main__":
    unittest.main()
