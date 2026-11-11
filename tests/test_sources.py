import json
import unittest
from pathlib import Path
from app.csvsource import read_header,read_rows
class DomainTests(unittest.TestCase):
    def test_header(self): self.assertEqual(read_header("fixtures/weekly.csv"),["district","week","quantity"])
    def test_zero(self): self.assertEqual(next(read_rows("fixtures/weekly.csv"))["quantity"],"0")
    def test_bom(self): self.assertEqual(read_header("fixtures/bom.csv")[0],"district")

class FixtureTests(unittest.TestCase):
    def test_fixture_directory(self): self.assertTrue(Path("fixtures").is_dir())
