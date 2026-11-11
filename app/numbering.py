"""Numeric normalisation that keeps the distinction between the reasons a
value is not a plain number.

An empty field is a declared null.  An explicit zero is not the same thing as
an empty field.  A value written with a declared thousands separator is
recorded as such (and still carries its numeric value).  Anything else is
invalid and is retained verbatim for the data maintainers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

_DIGITS = re.compile(r"[0-9]+")


@dataclass(frozen=True)
class NumberValue:
    kind: str  # null | zero | number | thousands | invalid
    raw: str
    value: str | None = None
    has_thousands_separator: bool = False
    is_zero: bool = False
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "raw": self.raw,
            "value": self.value,
            "has_thousands_separator": self.has_thousands_separator,
            "is_zero": self.is_zero,
            "detail": self.detail,
        }


def _invalid(raw: str, detail: str) -> NumberValue:
    return NumberValue(kind="invalid", raw=raw, detail=detail)


def normalize_number(
    raw: str,
    *,
    thousands_separator: str = ",",
    decimal_separator: str = ".",
) -> NumberValue:
    """Normalise one declared numeric field.

    The syntax is intentionally strict: a separator present in the field must
    create valid groups, so ``1,20`` is invalid rather than silently becoming
    ``120``.
    """

    text = raw.strip()
    if not text:
        return NumberValue(kind="null", raw=raw)

    sign = ""
    if text[0] in "+-":
        sign, text = text[0], text[1:]
    if not text:
        return _invalid(raw, "sign without digits")

    has_thousands = bool(thousands_separator and thousands_separator in text)

    # Split off the fractional part using the declared decimal separator.
    if decimal_separator in text:
        if text.count(decimal_separator) != 1:
            return _invalid(raw, "more than one decimal separator")
        integer_part, fraction = text.split(decimal_separator, 1)
    else:
        integer_part, fraction = text, None

    if fraction is not None and fraction != "" and not _DIGITS.fullmatch(fraction):
        return _invalid(raw, "non-digit character in fractional part")

    if has_thousands:
        if not thousands_separator or thousands_separator not in integer_part:
            # The separator only occurred in the fractional part, which is
            # always invalid.
            return _invalid(raw, "thousands separator in fractional part")
        groups = integer_part.split(thousands_separator)
        if (
            len(groups) < 2
            or not groups[0]
            or not _DIGITS.fullmatch(groups[0])
            or len(groups[0]) > 3
            or any(not _DIGITS.fullmatch(group) or len(group) != 3 for group in groups[1:])
        ):
            return _invalid(raw, "invalid thousands grouping")
        digits = "".join(groups)
    else:
        if integer_part == "":
            # A bare ".5" is accepted only when the decimal part exists.
            if fraction is None or fraction == "":
                return _invalid(raw, "missing digits")
            digits = ""
        elif not _DIGITS.fullmatch(integer_part):
            return _invalid(raw, "non-digit character in integer part")
        else:
            digits = integer_part

    if digits == "" and (fraction is None or fraction == ""):
        return _invalid(raw, "missing digits")

    canonical = f"{sign}{digits or '0'}"
    if fraction:
        canonical = f"{canonical}.{fraction}"
    try:
        value = Decimal(canonical)
    except InvalidOperation:
        return _invalid(raw, "not a decimal number")

    value_text = format(value, "f")
    if "." in value_text:
        value_text = value_text.rstrip("0").rstrip(".")
    if value_text in ("", "-0"):
        value_text = "0"

    is_zero = value == 0
    if is_zero:
        kind = "zero"
    elif has_thousands:
        kind = "thousands"
    else:
        kind = "number"

    return NumberValue(
        kind=kind,
        raw=raw,
        value=value_text,
        has_thousands_separator=has_thousands,
        is_zero=is_zero,
    )
