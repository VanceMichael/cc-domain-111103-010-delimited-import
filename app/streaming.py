"""Record-at-a-time delimited reading.

The reader never relies on the whole file fitting in memory.  Bytes are
decoded incrementally, so a buffer boundary in the middle of a multi-byte
character is harmless.  Physical lines are handed to :mod:`csv`, which
reassembles a quoted field containing newlines; the tracker then reports the
first and last physical line of each record.  Raw decoded text is retained for
audit and rejection messages.
"""

from __future__ import annotations

import codecs
import csv
import re
from pathlib import Path
from typing import Any, Iterator

from app.contract import FileContract

_LINE_END = re.compile(r"\r\n|\n|\r")
_HEADER_TOKEN = re.compile(r"^[A-Za-z_][A-Za-z0-9_ .\-]*$")


def iter_physical_lines(
    path: str | Path,
    encoding: str,
    chunk_size: int = 64 * 1024,
) -> Iterator[tuple[int, str]]:
    """Yield ``(physical_line_number, original_text)`` from a binary file.

    ``original_text`` includes the line terminator when one exists.  The
    incremental decoder is the key detail: ``read(chunk_size)`` may end in the
    middle of a multi-byte UTF-8 (or other) character and the remaining bytes
    are carried into the next chunk rather than producing a replacement or
    splitting the record.
    """

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    decoder = codecs.getincrementaldecoder(encoding)()
    line_number = 0
    pending = ""

    with Path(path).open("rb") as stream:
        while True:
            chunk = stream.read(chunk_size)
            if chunk:
                pending += decoder.decode(chunk, final=False)
            else:
                pending += decoder.decode(b"", final=True)

            position = 0
            while True:
                match = _LINE_END.search(pending, position)
                if match is None:
                    break
                end = match.end()
                # A trailing CR may be the first half of a CRLF split across
                # chunks.  Wait for one more chunk before calling it a line.
                if end == len(pending) and match.group() == "\r":
                    break
                line_number += 1
                yield line_number, pending[position:end]
                position = end
            pending = pending[position:]

            if not chunk:
                break

    if pending:
        line_number += 1
        yield line_number, pending


class QuoteTracker:
    """Tiny CSV quote-state machine used for comment handling and truncation.

    This is not a second CSV parser.  It only needs to answer two questions:
    "is this physical line inside an open quoted field?" and "did the file end
    while still inside a quoted field?"
    """

    def __init__(self, contract: FileContract) -> None:
        self.delimiter = contract.delimiter
        self.quotechar = contract.quotechar if contract.quoting != "none" else None
        self.doublequote = contract.doublequote
        self.escapechar = contract.escapechar
        self.skipinitialspace = contract.skipinitialspace
        self.state = "field_start"

    @property
    def in_quotes(self) -> bool:
        return self.state == "quoted"

    def feed(self, line: str) -> None:
        index = 0
        length = len(line)
        while index < length:
            char = line[index]
            if self.escapechar is not None and char == self.escapechar:
                index += 2
                continue
            if self.state == "quoted":
                if self.quotechar is not None and char == self.quotechar:
                    if (
                        self.doublequote
                        and index + 1 < length
                        and line[index + 1] == self.quotechar
                    ):
                        index += 2
                        continue
                    self.state = "after_quote"
            else:
                if char == self.delimiter:
                    self.state = "field_start"
                elif char in "\r\n":
                    self.state = "field_start"
                elif self.state == "field_start":
                    if self.skipinitialspace and char == " ":
                        pass
                    elif self.quotechar is not None and char == self.quotechar:
                        self.state = "quoted"
                    else:
                        self.state = "unquoted"
            index += 1

        if self.state != "quoted":
            # A record terminated at the end of this physical line.
            self.state = "field_start"


class CommentFilter:
    """Drop declared comment lines while preserving line numbers.

    Comment-looking text inside an open quoted field is data, not a comment.
    """

    def __init__(
        self, numbered_lines: Iterator[tuple[int, str]], contract: FileContract
    ) -> None:
        self._lines = iter(numbered_lines)
        self._contract = contract
        self.tracker = QuoteTracker(contract)
        self.events: list[dict[str, Any]] = []

    def __iter__(self) -> "CommentFilter":
        return self

    def __next__(self) -> tuple[int, str]:
        while True:
            number, line = next(self._lines)
            if not self.tracker.in_quotes and any(
                line.lstrip(" \t").startswith(prefix)
                for prefix in self._contract.comment_prefixes
                if prefix
            ):
                self.events.append(
                    {"kind": "comment", "line": number, "raw_text": line}
                )
                continue
            self.tracker.feed(line)
            return number, line

    def drain(self) -> list[dict[str, Any]]:
        events = self.events
        self.events = []
        return events


class LineTracker:
    """Expose line spans and raw text for each record csv consumes."""

    def __init__(self, numbered_lines: Iterator[tuple[int, str]]) -> None:
        self._lines = iter(numbered_lines)
        self.start_line: int | None = None
        self.end_line: int | None = None
        self._parts: list[str] = []

    def begin(self) -> None:
        self.start_line = None
        self.end_line = None
        self._parts = []

    def __iter__(self) -> "LineTracker":
        return self

    def __next__(self) -> str:
        number, line = next(self._lines)
        if self.start_line is None:
            self.start_line = number
        self.end_line = number
        self._parts.append(line)
        return line

    @property
    def raw_text(self) -> str:
        return "".join(self._parts)


def iter_events(
    path: str | Path,
    contract: FileContract,
    chunk_size: int = 64 * 1024,
) -> Iterator[dict[str, Any]]:
    """Yield comment, blank, record and parse-error events in file order."""

    comment_filter = CommentFilter(
        iter_physical_lines(path, contract.encoding, chunk_size), contract
    )
    tracker = LineTracker(comment_filter)
    reader = csv.reader(tracker, **contract.csv_kwargs())

    while True:
        tracker.begin()
        try:
            fields = next(reader)
        except StopIteration:
            yield from comment_filter.drain()
            break
        except csv.Error as error:
            yield from comment_filter.drain()
            yield {
                "kind": "parse_error",
                "line_start": tracker.start_line,
                "line_end": tracker.end_line,
                "raw_text": tracker.raw_text,
                "reason": str(error),
                "truncated": comment_filter.tracker.in_quotes,
            }
            break
        except (UnicodeDecodeError, LookupError) as error:
            yield from comment_filter.drain()
            yield {
                "kind": "decode_error",
                "line_start": tracker.start_line,
                "line_end": tracker.end_line,
                "raw_text": tracker.raw_text,
                "reason": str(error),
            }
            break

        yield from comment_filter.drain()

        if not fields:
            yield {
                "kind": "blank",
                "line_start": tracker.start_line,
                "line_end": tracker.end_line,
                "raw_text": tracker.raw_text,
            }
            continue

        yield {
            "kind": "record",
            "record": {
                "fields": list(fields),
                "line_start": tracker.start_line or 0,
                "line_end": tracker.end_line or 0,
                "raw_text": tracker.raw_text,
                "truncated": comment_filter.tracker.in_quotes,
            },
        }


def header_matches(fields: list[str], contract: FileContract) -> bool:
    expected = list(contract.expected_header)
    actual = [field.strip() for field in fields] if contract.strip_header else list(fields)
    return actual == expected


def looks_like_header(fields: list[str], contract: FileContract) -> bool:
    """Recognise a header-shaped row that is not the declared header.

    Used to pause a file whose header/format changed mid-stream instead of
    rejecting a single row and continuing to force values into the old
    contract.  Header names are ASCII tokens in the contracts this project
    ships; data rows in the fixtures are Chinese names or numeric values.
    """

    if not fields:
        return False
    known = {name.lower() for name in contract.expected_header}
    saw_known = False
    for field in fields:
        token = field.strip()
        if not token or _HEADER_TOKEN.fullmatch(token) is None:
            return False
        if token.lower() in known:
            saw_known = True
    return saw_known
