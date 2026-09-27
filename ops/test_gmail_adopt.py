import importlib.util
from pathlib import Path
import sqlite3
import tempfile
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


def verified_profile(ledger):
    rows = ledger.execute("""
        SELECT message_id, source_message_id, gmail_id, gmail_thread_id
        FROM capture WHERE status = 'mapped'
        ORDER BY message_id DESC LIMIT 3
    """).fetchall()
    return {"email": "owner@example.test", "history_id": "900",
            "archive_uid": "synthetic-archive", "source_id": 5,
            "verified_count": len(rows),
            "mapping_digest": module.mapping_digest(rows)}


def retained_fixture():
    archive, ledger = fixture()
    archive.executescript("""
        INSERT INTO message_raw VALUES (23, X'646966666572656E74');
        INSERT INTO attachments VALUES (32, 22);
        INSERT INTO message_embeddings VALUES (43, 23);
    """)
    ledger.execute("""UPDATE capture SET gmail_id = NULL,
        gmail_thread_id = NULL, status = 'missing' WHERE message_id = 22""")
    ledger.execute("""UPDATE capture SET gmail_id = 'a1',
        gmail_thread_id = 'f1' WHERE message_id = 23""")
    ledger.commit()
    manifest = {"archive_uid": "synthetic-archive", "source_id": 5,
                "retained_rows": [
                    {"message_id": 22, "source_message_id": "All Mail|2",
                     "gmail_id": None, "reason": "provider_absent"},
                    {"message_id": 23, "source_message_id": "All Mail|3",
                     "gmail_id": "a1", "reason": "duplicate_provider_id"}]}
    return archive, ledger, manifest


class AdoptTest(unittest.TestCase):
    def test_intervening_retained_or_source_identity_change_rejects_atomically(self):
        class InterposingArchive:
            def __init__(self, connection, before_lock):
                self.connection = connection
                self.before_lock = before_lock

            def execute(self, sql, arguments=()):
                if sql == "BEGIN IMMEDIATE":
                    self.before_lock()
                return self.connection.execute(sql, arguments)

            def __getattr__(self, name):
                return getattr(self.connection, name)

        for changed in ("retained_row", "source_identifier"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                fixture_archive, ledger, manifest = retained_fixture()
                profile = verified_profile(ledger)
                path = Path(directory) / "archive.db"
                main = sqlite3.connect(path)
                fixture_archive.backup(main)
                fixture_archive.close()
                writer = sqlite3.connect(path)

                def change_identity():
                    if changed == "retained_row":
                        writer.execute("""UPDATE messages SET source_message_id = ?
                            WHERE id = 22""", ("changed|2",))
                    else:
                        writer.execute("""UPDATE sources SET identifier = ?
                            WHERE id = 5""", (
                                "imaps://owner%40example.test@imap.gmail.com:993?changed=1",))
                    writer.commit()

                with self.assertRaisesRegex(ValueError, "identity changed"):
                    module.adopt(InterposingArchive(main, change_identity), ledger,
                                 profile, complete=True, retained_manifest=manifest)
                self.assertEqual("imap", main.execute(
                    "SELECT source_type FROM sources WHERE id = 5").fetchone()[0])
                self.assertEqual("All Mail|1", main.execute(
                    "SELECT source_message_id FROM messages WHERE id = 21").fetchone()[0])
                self.assertIsNone(main.execute("""SELECT name FROM sqlite_master
                    WHERE name = 'gmail_archive_only_adoption'""").fetchone())
                writer.close()
                main.close()
                ledger.close()

    def test_explicit_retained_rows_keep_archive_refs_and_canonical_provider_identity(self):
        archive, ledger, manifest = retained_fixture()
        old_raw = archive.execute(
            "SELECT message_id, raw_data FROM message_raw ORDER BY message_id").fetchall()
        result = module.adopt(archive, ledger, verified_profile(ledger),
                              complete=True, retained_manifest=manifest)
        self.assertEqual((1, 2), (result["mapped"], result["retained"]))
        self.assertEqual([(21, 11, "a1", 7, "old-stamp"),
                          (22, 12, "All Mail|2", 7, "old-stamp"),
                          (23, 12, "All Mail|3", 7, "old-stamp")],
                         archive.execute("""SELECT id, conversation_id,
                            source_message_id, embed_gen, last_modified
                            FROM messages ORDER BY id""").fetchall())
        self.assertEqual([22, 23], [row[0] for row in archive.execute("""
            SELECT message_id FROM gmail_archive_only_adoption ORDER BY message_id
        """)])
        self.assertEqual(old_raw, archive.execute(
            "SELECT message_id, raw_data FROM message_raw ORDER BY message_id").fetchall())
        self.assertEqual(2, archive.execute("SELECT COUNT(*) FROM attachments").fetchone()[0])
        self.assertEqual(3, archive.execute(
            "SELECT COUNT(*) FROM message_embeddings").fetchone()[0])
        self.assertEqual((11,), archive.execute("""
            SELECT conversation_id FROM gmail_thread_adoption
            WHERE source_id = 5 AND gmail_thread_id = 'f1'""").fetchone())
        # A native provider import using the exact Gmail ID targets only the
        # canonical row. The retained legacy copy cannot collide with it.
        archive.execute("""INSERT OR IGNORE INTO messages
            (id, source_id, conversation_id, source_message_id)
            VALUES (24, 5, 11, 'a1')""")
        self.assertEqual(3, archive.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
        with self.assertRaisesRegex(ValueError, "source identity"):
            module.adopt(archive, ledger, verified_profile(ledger),
                         complete=True, retained_manifest=manifest)
        archive.close()
        ledger.close()

    def test_retained_manifest_is_exact_and_failure_rolls_back(self):
        archive, ledger, manifest = retained_fixture()
        profile = verified_profile(ledger)
        for bad in (
                {**manifest, "source_id": 99},
                {**manifest, "retained_rows": manifest["retained_rows"][:1]},
                {**manifest, "retained_rows": [
                    {**manifest["retained_rows"][0], "source_message_id": "wrong"},
                    manifest["retained_rows"][1]]},
                {**manifest, "retained_rows": [
                    manifest["retained_rows"][0],
                    {**manifest["retained_rows"][1], "gmail_id": "a3"}]}):
            with self.assertRaises(ValueError):
                module.adopt(archive, ledger, profile, complete=True,
                             retained_manifest=bad)
            self.assertEqual("imap", archive.execute(
                "SELECT source_type FROM sources").fetchone()[0])
            self.assertEqual("All Mail|1", archive.execute(
                "SELECT source_message_id FROM messages WHERE id=21").fetchone()[0])
        ledger.execute("UPDATE capture SET gmail_thread_id = 'f2' WHERE message_id=23")
        ledger.commit()
        with self.assertRaisesRegex(ValueError, "matching canonical"):
            module.adopt(archive, ledger, verified_profile(ledger),
                         complete=True, retained_manifest=manifest)
        ledger.execute("UPDATE capture SET gmail_thread_id = 'f1' WHERE message_id=23")
        ledger.commit()
        # A late database failure must roll back both canonical rekey and
        # protected legacy markers, not expose a half-converted source.
        archive.execute("""CREATE TRIGGER fail_source_adoption
            BEFORE UPDATE ON sources BEGIN
            SELECT RAISE(ABORT, 'synthetic source update failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            module.adopt(archive, ledger, profile, complete=True,
                         retained_manifest=manifest)
        self.assertEqual("imap", archive.execute(
            "SELECT source_type FROM sources").fetchone()[0])
        self.assertEqual("All Mail|1", archive.execute(
            "SELECT source_message_id FROM messages WHERE id=21").fetchone()[0])
        self.assertIsNone(archive.execute("""SELECT name FROM sqlite_master
            WHERE name = 'gmail_archive_only_adoption'""").fetchone(),
            "failed transaction cannot leave a partial marker table")
        archive.close()
        ledger.close()

    def test_exact_rekey_preserves_message_raw_attachment_vector_and_thread_refs(self):
        archive, ledger = fixture()
        result = module.adopt(archive, ledger, verified_profile(ledger), complete=True)
        self.assertEqual(3, result["mapped"])
        self.assertEqual([(21, 11, "a1", 7, "old-stamp"),
                          (22, 12, "a2", 7, "old-stamp"),
                          (23, 12, "a3", 7, "old-stamp")],
                         archive.execute("""SELECT id, conversation_id,
                            source_message_id, embed_gen, last_modified
                            FROM messages ORDER BY id""").fetchall())
        self.assertEqual((5, "gmail", "owner@example.test", None, "{}"),
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
            module.adopt(archive, ledger, verified_profile(ledger), complete=True)
        self.assertEqual("imap", archive.execute("SELECT source_type FROM sources").fetchone()[0])
        ledger.execute("UPDATE capture SET gmail_id = 'a1' WHERE message_id = 22")
        with self.assertRaisesRegex(ValueError, "two archived rows"):
            module.adopt(archive, ledger, verified_profile(ledger), complete=False)
        self.assertEqual("All Mail|1", archive.execute(
            "SELECT source_message_id FROM messages WHERE id = 21").fetchone()[0])
        ledger.execute("UPDATE capture SET gmail_id = NULL, status = 'missing' WHERE message_id = 22")
        with self.assertRaisesRegex(ValueError, "unresolved"):
            module.adopt(archive, ledger, verified_profile(ledger), complete=False)
        archive.close()
        ledger.close()


if __name__ == "__main__":
    unittest.main()
