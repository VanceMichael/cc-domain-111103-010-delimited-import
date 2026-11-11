from pathlib import Path
from app.csvsource import read_rows
print(list(read_rows("fixtures/weekly.csv")))
