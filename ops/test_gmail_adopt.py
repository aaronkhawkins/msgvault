import importlib.util
from pathlib import Path
import sqlite3
import unittest


spec = importlib.util.spec_from_file_location(
    "gmail_adopt", Path(__file__).with_name("gmail_adopt.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixture():
    archive = sqlite3.connect(":memory:")
    archive.execute("PRAGMA foreign_keys = ON")
    archive.executescript("""
        CREATE TABLE archive_metadata (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE sources (id INTEGER PRIMARY KEY, source_type TEXT,
            identifier TEXT, sync_cursor TEXT, sync_config TEXT,
            UNIQUE(source_type, identifier));
        CREATE TABLE conversations (id INTEGER PRIMARY KEY,
            source_id INTEGER REFERENCES sources(id), source_conversation_id TEXT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, source_id INTEGER,
            conversation_id INTEGER, source_message_id TEXT, embed_gen INTEGER,
            last_modified TEXT, UNIQUE(source_id, source_message_id));
        CREATE TABLE message_raw (message_id INTEGER, raw_data BLOB);
        CREATE TABLE attachments (id INTEGER PRIMARY KEY, message_id INTEGER);
        CREATE TABLE message_embeddings (id INTEGER PRIMARY KEY, message_id INTEGER);
        INSERT INTO archive_metadata VALUES ('archive_uid', 'synthetic-archive');
        INSERT INTO sources VALUES
          (5, 'imap', 'imaps://owner%40example.test@imap.gmail.com:993', NULL, '{}');
        INSERT INTO conversations VALUES (11, 5, 'old-a'), (12, 5, 'old-b');
        INSERT INTO messages VALUES
          (21, 5, 11, 'All Mail|1', 7, 'old-stamp'),
          (22, 5, 12, 'All Mail|2', 7, 'old-stamp'),
          (23, 5, 12, 'All Mail|3', 7, 'old-stamp');
        INSERT INTO message_raw VALUES (21, X'616263'), (22, X'646566');
        INSERT INTO attachments VALUES (31, 21);
        INSERT INTO message_embeddings VALUES (41, 21), (42, 22);
    """)
    ledger = sqlite3.connect(":memory:")
    ledger.executescript("""
        CREATE TABLE migration (archive_uid TEXT, source_id INTEGER,
            next_before_id INTEGER);
        CREATE TABLE capture (message_id INTEGER, source_message_id TEXT,
            gmail_id TEXT, gmail_thread_id TEXT, status TEXT);
        INSERT INTO migration VALUES ('synthetic-archive', 5, 21);
        INSERT INTO capture VALUES
          (21, 'All Mail|1', 'a1', 'f1', 'mapped'),
          (22, 'All Mail|2', 'a2', 'f1', 'mapped'),
          (23, 'All Mail|3', 'a3', 'f2', 'mapped');
    """)
    return archive, ledger


class AdoptTest(unittest.TestCase):
    def test_exact_rekey_preserves_message_raw_attachment_vector_and_thread_refs(self):
        archive, ledger = fixture()
        result = module.adopt(archive, ledger,
                              {"email": "owner@example.test", "history_id": "900"},
                              complete=True)
        self.assertEqual(3, result["mapped"])
        self.assertEqual([(21, 11, "a1", 7, "old-stamp"),
                          (22, 12, "a2", 7, "old-stamp"),
                          (23, 12, "a3", 7, "old-stamp")],
                         archive.execute("""SELECT id, conversation_id,
                            source_message_id, embed_gen, last_modified
                            FROM messages ORDER BY id""").fetchall())
        self.assertEqual((5, "gmail", "owner@example.test", "900", None),
                         archive.execute("""SELECT id, source_type, identifier,
                            sync_cursor, sync_config FROM sources""").fetchone())
        self.assertEqual((11,), archive.execute("""
            SELECT conversation_id FROM gmail_thread_adoption
            WHERE source_id = 5 AND gmail_thread_id = 'f1'""").fetchone())
        self.assertEqual(2, archive.execute("SELECT COUNT(*) FROM message_raw").fetchone()[0])
        self.assertEqual(1, archive.execute("SELECT COUNT(*) FROM attachments").fetchone()[0])
        self.assertEqual(2, archive.execute("SELECT COUNT(*) FROM message_embeddings").fetchone()[0])
        archive.close()
        ledger.close()

    def test_incomplete_or_conflicting_ledger_does_not_convert(self):
        archive, ledger = fixture()
        ledger.execute("DELETE FROM capture WHERE message_id = 23")
        with self.assertRaisesRegex(ValueError, "does not cover"):
            module.adopt(archive, ledger,
                         {"email": "owner@example.test", "history_id": "900"},
                         complete=True)
        self.assertEqual("imap", archive.execute("SELECT source_type FROM sources").fetchone()[0])
        ledger.execute("UPDATE capture SET gmail_id = 'a1' WHERE message_id = 22")
        with self.assertRaisesRegex(ValueError, "two archived rows"):
            module.adopt(archive, ledger,
                         {"email": "owner@example.test", "history_id": "900"},
                         complete=False)
        self.assertEqual("All Mail|1", archive.execute(
            "SELECT source_message_id FROM messages WHERE id = 21").fetchone()[0])
        ledger.execute("UPDATE capture SET gmail_id = NULL, status = 'missing' WHERE message_id = 22")
        with self.assertRaisesRegex(ValueError, "unresolved"):
            module.adopt(archive, ledger,
                         {"email": "owner@example.test", "history_id": "900"},
                         complete=False)
        archive.close()
        ledger.close()


if __name__ == "__main__":
    unittest.main()
