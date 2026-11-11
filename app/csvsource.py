import csv
from pathlib import Path

def read_header(path,encoding="utf-8-sig",delimiter=","):
    with Path(path).open(encoding=encoding,newline="") as stream:
        return next(csv.reader(stream,delimiter=delimiter))

def read_rows(path,encoding="utf-8-sig",delimiter=","):
    with Path(path).open(encoding=encoding,newline="") as stream:
        yield from csv.DictReader(stream,delimiter=delimiter)
