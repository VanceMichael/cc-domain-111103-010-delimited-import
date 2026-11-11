import unittest
from dataclasses import replace

from app.contract import FileContract


class ContractTests(unittest.TestCase):
    def test_declared_config_round_trips(self):
        contract = FileContract.load("configs/weekly_comma.json")
        self.assertEqual(contract.delimiter, ",")
        self.assertEqual(contract.encoding, "utf-8-sig")
        self.assertEqual(contract.quoting, "minimal")
        self.assertEqual(contract.expected_header, ("district", "week", "quantity"))
        self.assertEqual(contract.columns[-1].kind, "number")

    def test_contract_identity_changes_with_rules(self):
        base = FileContract.load("configs/weekly_comma.json")
        semicolon = FileContract.load("configs/weekly_semicolon.json")
        self.assertNotEqual(base.fingerprint(), semicolon.fingerprint())
        self.assertNotEqual(base.run_key(), semicolon.run_key())

        changed_version = replace(base, version="1.1")
        self.assertNotEqual(base.fingerprint(), changed_version.fingerprint())

        changed_quote = replace(base, quotechar="'")
        self.assertNotEqual(base.fingerprint(), changed_quote.fingerprint())

    def test_quote_none_normalises_quotechar(self):
        base = FileContract.load("configs/weekly_comma.json")
        contract = replace(base, quoting="none", quotechar='"')
        self.assertIsNone(contract.quotechar)
        self.assertEqual(contract.csv_kwargs()["quoting"], 3)


if __name__ == "__main__":
    unittest.main()
