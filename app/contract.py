"""Explicit file contracts for delimited supply reports.

The contract is deliberately declarative.  Nothing in the importer sniffs a
file to decide its delimiter, encoding, quoting rules, or columns: an operator
must select a contract.  A malformed or unexpectedly shaped file is therefore
visible as a contract/format problem instead of being squeezed into a guessed
schema.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_QUOTING = {
    "minimal": csv.QUOTE_MINIMAL,
    "all": csv.QUOTE_ALL,
    "nonnumeric": csv.QUOTE_NONNUMERIC,
    "none": csv.QUOTE_NONE,
}
_COLUMN_KINDS = {"text", "number", "integer"}
_ROW_POLICIES = {"publish_valid", "hold_all"}


@dataclass(frozen=True)
class Column:
    """One declared column in a report contract."""

    name: str
    kind: str = "text"
    required: bool = False
    allow_empty: bool = True

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("column name must not be empty")
        if self.kind not in _COLUMN_KINDS:
            raise ValueError(f"unsupported column kind: {self.kind!r}")

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Column":
        return cls(
            name=str(raw["name"]),
            kind=str(raw.get("kind", "text")),
            required=bool(raw.get("required", False)),
            allow_empty=bool(raw.get("allow_empty", True)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "required": self.required,
            "allow_empty": self.allow_empty,
        }

    @property
    def rejects_empty(self) -> bool:
        """Whether an empty value is a contract violation for this column."""

        return self.required or not self.allow_empty


@dataclass(frozen=True)
class FileContract:
    """All format rules for one class of source file.

    The canonical representation (and therefore the replay identity) includes
    every rule below.  A corrected contract always gets a new identity, so its
    output can never overwrite an earlier run.
    """

    name: str
    version: str
    delimiter: str
    encoding: str
    columns: tuple[Column, ...]
    quotechar: str | None = '"'
    quoting: str = "minimal"
    doublequote: bool = True
    escapechar: str | None = None
    skipinitialspace: bool = False
    comment_prefixes: tuple[str, ...] = ("#",)
    thousands_separator: str = ","
    decimal_separator: str = "."
    row_error_policy: str = "publish_valid"
    strip_header: bool = True

    def __post_init__(self) -> None:
        if not self.name or not self.version:
            raise ValueError("contract name and version are required")
        if len(self.delimiter) != 1:
            raise ValueError("delimiter must be a single character")
        if not self.encoding:
            raise ValueError("encoding must be declared")
        if self.quoting not in _QUOTING:
            raise ValueError(f"unsupported quoting mode: {self.quoting!r}")
        if self.quoting == "none" and self.quotechar is not None:
            # A quote character is meaningless in QUOTE_NONE mode; normalising
            # it keeps the contract identity stable.
            object.__setattr__(self, "quotechar", None)
        if self.quoting != "none" and not self.quotechar:
            raise ValueError("quotechar is required unless quoting is 'none'")
        if self.escapechar is not None and len(self.escapechar) != 1:
            raise ValueError("escapechar must be one character or null")
        if self.thousands_separator and len(self.thousands_separator) != 1:
            raise ValueError("thousands_separator must be one character or empty")
        if not self.decimal_separator:
            raise ValueError("decimal_separator must be declared")
        if self.thousands_separator == self.decimal_separator:
            raise ValueError("thousands and decimal separators must differ")
        if self.row_error_policy not in _ROW_POLICIES:
            raise ValueError(f"unsupported row_error_policy: {self.row_error_policy!r}")
        if not self.columns:
            raise ValueError("at least one column is required")
        names = [column.name for column in self.columns]
        if len(set(names)) != len(names):
            raise ValueError("column names must be unique")
        if any(prefix == "" for prefix in self.comment_prefixes):
            raise ValueError("comment prefixes must not be empty")

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "FileContract":
        raw_columns = raw.get("columns")
        if not isinstance(raw_columns, list) or not raw_columns:
            raise ValueError("contract must declare a non-empty columns list")
        columns = tuple(Column.from_dict(item) for item in raw_columns)
        return cls(
            name=str(raw["name"]),
            version=str(raw["version"]),
            delimiter=str(raw["delimiter"]),
            encoding=str(raw["encoding"]),
            columns=columns,
            quotechar=raw.get("quotechar", '"'),
            quoting=str(raw.get("quoting", "minimal")),
            doublequote=bool(raw.get("doublequote", True)),
            escapechar=raw.get("escapechar"),
            skipinitialspace=bool(raw.get("skipinitialspace", False)),
            comment_prefixes=tuple(raw.get("comment_prefixes", ["#"])),
            thousands_separator=str(raw.get("thousands_separator", ",")),
            decimal_separator=str(raw.get("decimal_separator", ".")),
            row_error_policy=str(raw.get("row_error_policy", "publish_valid")),
            strip_header=bool(raw.get("strip_header", True)),
        )

    @classmethod
    def load(cls, path: str | Path) -> "FileContract":
        with Path(path).open(encoding="utf-8") as stream:
            raw = json.load(stream)
        return cls.from_dict(raw)

    @property
    def expected_header(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def csv_kwargs(self) -> dict[str, Any]:
        return {
            "delimiter": self.delimiter,
            "quotechar": self.quotechar,
            "quoting": _QUOTING[self.quoting],
            "doublequote": self.doublequote,
            "escapechar": self.escapechar,
            "skipinitialspace": self.skipinitialspace,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "delimiter": self.delimiter,
            "encoding": self.encoding,
            "columns": [column.to_dict() for column in self.columns],
            "quotechar": self.quotechar,
            "quoting": self.quoting,
            "doublequote": self.doublequote,
            "escapechar": self.escapechar,
            "skipinitialspace": self.skipinitialspace,
            "comment_prefixes": list(self.comment_prefixes),
            "thousands_separator": self.thousands_separator,
            "decimal_separator": self.decimal_separator,
            "row_error_policy": self.row_error_policy,
            "strip_header": self.strip_header,
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )

    def fingerprint(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def run_key(self) -> str:
        """Stable output-directory name for this contract identity."""

        return f"{self.name}-v{self.version}-{self.fingerprint()[:12]}"

    def describe(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "fingerprint": self.fingerprint(),
            "delimiter": self.delimiter,
            "encoding": self.encoding,
            "quoting": self.quoting,
            "quotechar": self.quotechar,
            "columns": list(self.expected_header),
        }
