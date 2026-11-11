import tempfile
import unittest
from pathlib import Path

from app.contract import FileContract
from app.streaming import iter_events

COMMA = FileContract.load("configs/weekly_comma.json")
SEMICOLON = FileContract.load("configs/weekly_semicolon.json")


def records(path, contract, chunk_size):
    return [
        event["record"]
        for event in iter_events(path, contract, chunk_size=chunk_size)
        if event["kind"] == "record"
    ]


class StreamingTests(unittest.TestCase):
    def test_chunk_boundaries_do_not_change_records(self):
        cases = [
            ("fixtures/weekly.csv", COMMA),
            ("fixtures/bom.csv", COMMA),
            ("fixtures/semicolon.csv", SEMICOLON),
            ("fixtures/single_byte_cp1252.csv", FileContract.load("configs/weekly_single_byte.json")),
        ]
        for path, contract in cases:
            with self.subTest(path=path):
                baseline = records(path, contract, 64 * 1024)
                for chunk_size in (1, 2, 3, 7, 13, 64):
                    observed = records(path, contract, chunk_size)
                    self.assertEqual(observed, baseline, f"chunk_size={chunk_size}")

    def test_multibyte_character_split_across_chunks(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "multibyte.csv"
            path.write_text(
                "district,week,quantity\n河谷,45,0\n山地,46,12\n",
                encoding="utf-8",
            )
            baseline = records(path, COMMA, 64 * 1024)
            observed = records(path, COMMA, 1)
            self.assertEqual(observed, baseline)
            self.assertEqual(observed[1]["fields"][0], "河谷")
            self.assertEqual(observed[2]["fields"][0], "山地")

    def test_quoted_newline_keeps_physical_lines_and_raw_text(self):
        value = records("fixtures/semicolon.csv", SEMICOLON, 1)[1]
        self.assertEqual(value["fields"], ["山地", "45", "17\n18"])
        self.assertEqual(value["line_start"], 2)
        self.assertEqual(value["line_end"], 3)
        self.assertEqual(value["raw_text"], '山地;45;"17\n18"\n')
        self.assertFalse(value["truncated"])

    def test_comment_like_text_inside_quotes_is_data(self):
        contract = SEMICOLON
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quoted_comment.csv"
            path.write_text(
                'district;week;quantity\n山地;45;"# not a comment\n18"\n',
                encoding="utf-8",
            )
            events = list(iter_events(path, contract, chunk_size=1))
            self.assertEqual([event["kind"] for event in events], ["record", "record"])
            value = events[1]["record"]
            self.assertEqual(value["fields"][2], "# not a comment\n18")
            self.assertEqual((value["line_start"], value["line_end"]), (2, 3))

    def test_truncated_quoted_field_is_flagged(self):
        value = records("fixtures/truncated.csv", COMMA, 1)[-1]
        self.assertTrue(value["truncated"])
        self.assertEqual(value["line_start"], 3)
        self.assertEqual(value["line_end"], 3)
        self.assertIn('"1,20', value["raw_text"])


if __name__ == "__main__":
    unittest.main()
