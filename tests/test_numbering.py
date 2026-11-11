import unittest

from app.numbering import normalize_number


class NumberTests(unittest.TestCase):
    def test_empty_is_null(self):
        for raw in ("", "   "):
            value = normalize_number(raw)
            self.assertEqual(value.kind, "null")
            self.assertIsNone(value.value)
            self.assertFalse(value.is_zero)

    def test_explicit_zero_is_not_null(self):
        for raw, has_thousands in (("0", False), ("0.00", False), ("-0", False), ("0,000", True)):
            value = normalize_number(raw)
            self.assertEqual(value.kind, "zero", raw)
            self.assertEqual(value.value, "0")
            self.assertTrue(value.is_zero)
            self.assertEqual(value.has_thousands_separator, has_thousands)

    def test_thousands_separator_is_recorded(self):
        value = normalize_number("1,205")
        self.assertEqual(value.kind, "thousands")
        self.assertEqual(value.value, "1205")
        self.assertTrue(value.has_thousands_separator)

        value = normalize_number("12,345,678.50")
        self.assertEqual(value.kind, "thousands")
        self.assertEqual(value.value, "12345678.5")

    def test_plain_number(self):
        value = normalize_number("1205")
        self.assertEqual(value.kind, "number")
        self.assertEqual(value.value, "1205")
        self.assertFalse(value.has_thousands_separator)

    def test_invalid_numbers(self):
        for raw in ("abc", "1,20", "12,34,567", ",205", "1.2.3", "1,205,", "12 34"):
            value = normalize_number(raw)
            self.assertEqual(value.kind, "invalid", raw)
            self.assertIsNone(value.value)

    def test_declared_separators(self):
        value = normalize_number("1.205,50", thousands_separator=".", decimal_separator=",")
        self.assertEqual(value.kind, "thousands")
        self.assertEqual(value.value, "1205.5")


if __name__ == "__main__":
    unittest.main()
