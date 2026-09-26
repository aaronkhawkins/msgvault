import importlib.util
from pathlib import Path
import sys
import unittest


ops = Path(__file__).parent
sys.path.insert(0, str(ops))
from test_gmail_adopt import fixture  # noqa: E402

spec = importlib.util.spec_from_file_location("gmail_api_pilot", ops / "gmail_api_pilot.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PilotTest(unittest.TestCase):
    def test_provider_id_thread_and_existing_archive_are_checked_without_writes(self):
        archive, ledger = fixture()
        calls = []

        def get(_token, path):
            calls.append(path)
            if path == "/profile":
                return {"emailAddress": "owner@example.test", "historyId": "900"}
            gmail_id = path.split("/messages/", 1)[1].split("?", 1)[0]
            return {"id": gmail_id,
                    "threadId": "f1" if gmail_id in ("a1", "a2") else "f2"}

        profile, result = module.pilot(archive, ledger, {
            "access_token": "synthetic",
            "scopes": [module.GMAIL_READONLY_SCOPE]}, limit=3, request=get)
        self.assertEqual("owner@example.test", profile["email"])
        self.assertEqual("900", profile["history_id"])
        self.assertEqual("synthetic-archive", profile["archive_uid"])
        self.assertEqual(5, profile["source_id"])
        self.assertEqual(3, profile["verified_count"])
        self.assertEqual(64, len(profile["mapping_digest"]))
        self.assertEqual(3, result["api_ids_and_threads_verified"])
        self.assertEqual(2, result["existing_raw_messages"])
        self.assertEqual(1, result["existing_attachment_messages"])
        self.assertEqual(0, result["archive_rows_mutated"])
        self.assertEqual(4, len(calls))
        self.assertEqual("imap", archive.execute(
            "SELECT source_type FROM sources").fetchone()[0])
        archive.close()
        ledger.close()

    def test_mismatched_provider_thread_rejects_pilot(self):
        archive, ledger = fixture()

        def get(_token, path):
            if path == "/profile":
                return {"emailAddress": "owner@example.test", "historyId": "900"}
            return {"id": "a3", "threadId": "wrong"}

        with self.assertRaisesRegex(ValueError, "identity disagrees"):
            module.pilot(archive, ledger, {
                "access_token": "synthetic",
                "scopes": [module.GMAIL_READONLY_SCOPE]}, limit=1, request=get)
        archive.close()
        ledger.close()


if __name__ == "__main__":
    unittest.main()
