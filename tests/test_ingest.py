import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from app.contract import FileContract
from app.ingest import BatchStore, IngestError, ingest_file, reprocess

COMMA = FileContract.load("configs/weekly_comma.json")
SEMICOLON = FileContract.load("configs/weekly_semicolon.json")
SINGLE_BYTE = FileContract.load("configs/weekly_single_byte.json")
SUPPLIER = FileContract.load("configs/weekly_supplier.json")


def read_jsonl(path):
    with Path(path).open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = BatchStore(Path(self.temp.name) / "runtime")

    def tearDown(self):
        self.temp.cleanup()

    def ingest(self, name, contract=COMMA, store=None):
        return ingest_file(Path("fixtures") / name, contract, store or self.store)

    def test_complete_batch_and_raw_trace(self):
        result = self.ingest("weekly.csv")
        manifest = result.manifest
        self.assertTrue(result.created)
        self.assertEqual(manifest["status"], "complete")
        self.assertTrue(manifest["complete"])
        self.assertTrue(manifest["published"])
        self.assertEqual(manifest["counts"]["accepted"], 2)
        self.assertEqual(manifest["rejections"], {})

        accepted = read_jsonl(result.batch_dir / "accepted.jsonl")
        self.assertEqual(len(accepted), 2)
        for record in accepted:
            self.assertEqual(record["source_digest"], result.source_digest)
            self.assertEqual(len(record["raw_fields"]), 3)
            self.assertTrue(record["raw_text"].endswith("\n"))

        quantities = [record["normalized"]["quantity"] for record in accepted]
        self.assertEqual(quantities[0]["kind"], "zero")
        self.assertEqual(quantities[0]["raw"], "0")
        self.assertEqual(quantities[1]["kind"], "thousands")
        self.assertEqual(quantities[1]["raw"], "1,205")
        self.assertEqual(quantities[1]["value"], "1205")

    def test_bom_stripped(self):
        result = self.ingest("bom.csv")
        self.assertEqual(result.manifest["format"]["observed_header"], ["district", "week", "quantity"])
        self.assertEqual(result.manifest["status"], "complete")

    def test_comments_and_repeated_headers_are_skipped(self):
        result = self.ingest("notes.csv")
        counts = result.manifest["counts"]
        self.assertEqual(counts["comments"], 1)
        self.assertEqual(counts["repeated_headers"], 1)
        self.assertEqual(counts["accepted"], 2)
        self.assertEqual(result.manifest["status"], "complete")
        skipped = read_jsonl(result.batch_dir / "skipped.jsonl")
        kinds = [item["kind"] for item in skipped]
        self.assertEqual(kinds, ["comment", "repeated_header"])
        self.assertEqual(skipped[1]["line_start"], 4)

    def test_quoted_newline_rejection_keeps_original_text(self):
        result = self.ingest("semicolon.csv", SEMICOLON)
        self.assertEqual(result.manifest["status"], "partial")
        self.assertFalse(result.manifest["complete"])
        self.assertEqual(result.manifest["rejections"], {"invalid_number": 1})
        rejected = read_jsonl(result.batch_dir / "rejected.jsonl")
        self.assertEqual(len(rejected), 1)
        record = rejected[0]
        self.assertEqual(record["reason_code"], "invalid_number")
        self.assertEqual((record["line_start"], record["line_end"]), (2, 3))
        self.assertEqual(record["raw_fields"], ["山地", "45", "17\n18"])
        self.assertIn("17\n18", record["raw_text"])
        self.assertEqual(record["normalized"]["quantity"]["kind"], "invalid")
        self.assertTrue((result.batch_dir / "errors" / "invalid_number.jsonl").is_file())

    def test_truncated_file_is_not_published(self):
        result = self.ingest("truncated.csv")
        self.assertEqual(result.manifest["status"], "incomplete")
        self.assertFalse(result.manifest["complete"])
        self.assertFalse(result.manifest["published"])
        self.assertEqual(result.manifest["counts"]["accepted"], 1)
        rejected = read_jsonl(result.batch_dir / "rejected.jsonl")
        self.assertEqual(rejected[-1]["reason_code"], "truncated_quoted_field")
        self.assertEqual((rejected[-1]["line_start"], rejected[-1]["line_end"]), (3, 3))
        self.assertIn('"1,20', rejected[-1]["raw_text"])
        # Valid rows stay available for review, but publication is held.
        self.assertEqual(len(read_jsonl(result.batch_dir / "accepted.jsonl")), 1)

    def test_mid_file_header_change_quarantines_file(self):
        result = self.ingest("format_change.csv")
        manifest = result.manifest
        self.assertEqual(manifest["status"], "quarantined")
        self.assertFalse(manifest["complete"])
        self.assertFalse(manifest["published"])
        issue = manifest["issues"][0]
        self.assertEqual(issue["code"], "header_change")
        self.assertEqual(issue["line_start"], 3)
        self.assertEqual(issue["observed"], ["region", "week", "amount"])

    def test_wrong_contract_header_is_quarantined(self):
        result = self.ingest("semicolon_ok.csv", COMMA)
        self.assertEqual(result.manifest["status"], "quarantined")
        self.assertEqual(result.manifest["issues"][0]["code"], "header_mismatch")
        self.assertEqual(result.manifest["counts"]["accepted"], 0)

    def test_single_byte_encoding(self):
        result = self.ingest("single_byte_cp1252.csv", SINGLE_BYTE)
        self.assertEqual(result.manifest["status"], "complete")
        accepted = read_jsonl(result.batch_dir / "accepted.jsonl")
        self.assertEqual(accepted[0]["normalized"]["district"]["value"], "Rivière")

    def test_empty_supplier_is_allowed(self):
        result = self.ingest("supplier_empty.csv", SUPPLIER)
        self.assertEqual(result.manifest["status"], "complete")
        accepted = read_jsonl(result.batch_dir / "accepted.jsonl")
        self.assertEqual(accepted[0]["normalized"]["supplier"]["kind"], "empty")
        self.assertIsNone(accepted[0]["normalized"]["supplier"]["value"])

    def test_idempotent_same_file_and_renamed_file(self):
        first = self.ingest("weekly.csv")
        second = self.ingest("weekly.csv")
        self.assertFalse(second.created)
        self.assertTrue(second.duplicate)
        self.assertEqual(first.batch_dir, second.batch_dir)

        renamed = Path(self.temp.name) / "weekly_renamed.csv"
        renamed.write_bytes(Path("fixtures/weekly.csv").read_bytes())
        third = self.ingest(renamed)
        self.assertFalse(third.created)
        self.assertTrue(third.duplicate)
        self.assertEqual(first.batch_dir, third.batch_dir)
        self.assertIn("weekly_renamed.csv", self.store.source_meta(first.digest_hex)["filenames"])
        self.assertEqual(len(list(self.store.manifests())), 1)

    def test_contract_correction_replays_without_overwriting_old_run(self):
        wrong = self.ingest("semicolon_ok.csv", COMMA)
        self.assertEqual(wrong.manifest["status"], "quarantined")
        old_manifest_path = wrong.batch_dir / "manifest.json"
        old_bytes = old_manifest_path.read_bytes()
        old_files = sorted(path.name for path in wrong.batch_dir.iterdir())

        replay = reprocess(wrong.source_digest, SEMICOLON, self.store)
        self.assertTrue(replay.created)
        self.assertEqual(replay.manifest["status"], "complete")
        self.assertTrue(replay.manifest["published"])
        self.assertNotEqual(replay.batch_dir, wrong.batch_dir)
        self.assertEqual(replay.source_digest, wrong.source_digest)

        self.assertEqual(old_manifest_path.read_bytes(), old_bytes)
        self.assertEqual(sorted(path.name for path in wrong.batch_dir.iterdir()), old_files)
        self.assertEqual(len(list(self.store.manifests())), 2)

        again = reprocess(wrong.source_digest, SEMICOLON, self.store)
        self.assertFalse(again.created)
        self.assertEqual(len(list(self.store.manifests())), 2)

    def test_hold_all_policy_withholds_partial_batch(self):
        contract = replace(SEMICOLON, row_error_policy="hold_all")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "mixed.csv"
            path.write_text(
                'district;week;quantity\n丘陵;45;12\n丘陵;46;"17\n18"\n',
                encoding="utf-8",
            )
            result = ingest_file(path, contract, self.store)
        self.assertEqual(result.manifest["status"], "partial")
        self.assertFalse(result.manifest["complete"])
        self.assertFalse(result.manifest["published"])
        self.assertEqual(result.manifest["counts"]["accepted"], 1)
        self.assertEqual(result.manifest["counts"]["rejected"], 1)

    def test_publish_valid_policy_publishes_partial_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "mixed.csv"
            path.write_text(
                'district;week;quantity\n丘陵;45;12\n丘陵;46;"17\n18"\n',
                encoding="utf-8",
            )
            result = ingest_file(path, SEMICOLON, self.store)
        self.assertEqual(result.manifest["status"], "partial")
        self.assertFalse(result.manifest["complete"])
        self.assertTrue(result.manifest["published"])
        self.assertEqual(result.manifest["counts"]["accepted"], 1)

    def test_declared_encoding_failure_is_quarantined(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid_utf8.csv"
            path.write_bytes(b"district,week,quantity\n\xff,45,12\n")
            result = ingest_file(path, COMMA, self.store)
        self.assertEqual(result.manifest["status"], "quarantined")
        self.assertEqual(result.manifest["issues"][0]["code"], "decode_error")
        self.assertFalse(result.manifest["published"])

    def test_source_bytes_are_preserved(self):
        result = self.ingest("single_byte_cp1252.csv", SINGLE_BYTE)
        captured = self.store.source_path(result.digest_hex)
        self.assertEqual(captured.read_bytes(), Path("fixtures/single_byte_cp1252.csv").read_bytes())

    def test_unknown_digest_replay_is_rejected(self):
        with self.assertRaises(IngestError):
            reprocess("sha256:" + "0" * 64, COMMA, self.store)


if __name__ == "__main__":
    unittest.main()
