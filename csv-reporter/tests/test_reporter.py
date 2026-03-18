"""Tests for reporter.py — CSV analysis pipeline."""

import json
import textwrap
from pathlib import Path

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURE_CSV_CONTENT = textwrap.dedent("""\
    id,name,age,salary,department,hire_date
    1,Alice,30,75000,Engineering,2020-01-15
    2,Bob,25,55000,Marketing,2021-06-01
    3,Carol,35,90000,Engineering,2019-03-22
    4,Dave,28,62000,Sales,2022-08-10
    5,Eve,32,81000,Engineering,2020-11-30
    6,Frank,29,59000,Marketing,2021-02-14
    7,Grace,31,77000,Sales,2019-09-01
    8,Heidi,,68000,Engineering,2023-01-05
    9,Ivan,27,52000,Sales,2022-04-18
    10,Judy,33,88000,Engineering,2018-07-07
""")

LATIN1_CSV_CONTENT = "name,city\nCaf\xe9,Paris\nNa\xefve,Lyon\n"


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    """Write a small valid CSV fixture and return its path."""
    p = tmp_path / "sample.csv"
    p.write_text(FIXTURE_CSV_CONTENT, encoding="utf-8")
    return p


@pytest.fixture()
def latin1_csv(tmp_path: Path) -> Path:
    """Write a latin-1 encoded CSV fixture and return its path."""
    p = tmp_path / "latin1.csv"
    p.write_bytes(LATIN1_CSV_CONTENT.encode("latin-1"))
    return p


# ---------------------------------------------------------------------------
# Import helpers (keep reporter import after fixtures)
# ---------------------------------------------------------------------------

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from reporter import (
    analyze_dataframe,
    classify_columns,
    detect_encoding,
    load_csv,
    process_file,
    write_csv_summary,
    write_html,
    write_json,
)


# ---------------------------------------------------------------------------
# Load / validation tests
# ---------------------------------------------------------------------------

class TestLoadCsv:
    """Tests for load_csv()."""

    def test_load_valid_file(self, sample_csv: Path) -> None:
        """Should load a valid CSV and return a non-empty DataFrame."""
        df = load_csv(str(sample_csv))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert "name" in df.columns
        assert "salary" in df.columns

    def test_load_missing_file_raises(self, tmp_path: Path) -> None:
        """Should raise FileNotFoundError when file does not exist."""
        missing = str(tmp_path / "nonexistent.csv")
        with pytest.raises(FileNotFoundError, match="File not found"):
            load_csv(missing)

    def test_load_preserves_row_count(self, sample_csv: Path) -> None:
        """Row count in loaded DataFrame must match fixture."""
        df = load_csv(str(sample_csv))
        assert df.shape == (10, 6)


class TestEncodingDetection:
    """Tests for detect_encoding()."""

    def test_detects_utf8(self, sample_csv: Path) -> None:
        """Standard CSV should be detected as utf-8."""
        assert detect_encoding(str(sample_csv)) == "utf-8"

    def test_detects_latin1(self, latin1_csv: Path) -> None:
        """Latin-1 CSV with non-ASCII chars should fall back to latin-1."""
        assert detect_encoding(str(latin1_csv)) == "latin-1"

    def test_latin1_file_loads(self, latin1_csv: Path) -> None:
        """Latin-1 file should load successfully after fallback."""
        df = load_csv(str(latin1_csv))
        assert len(df) == 2
        assert "name" in df.columns


# ---------------------------------------------------------------------------
# Analysis structure tests
# ---------------------------------------------------------------------------

class TestAnalysis:
    """Tests for classify_columns() and analyze_dataframe()."""

    @pytest.fixture(autouse=True)
    def _setup(self, sample_csv: Path) -> None:
        self.df = load_csv(str(sample_csv))
        self.col_types = classify_columns(self.df)
        self.analysis = analyze_dataframe(self.df, self.col_types)

    def test_classify_numeric(self) -> None:
        """id, age, salary should be numeric."""
        assert "salary" in self.col_types["numeric"]
        assert "id" in self.col_types["numeric"]

    def test_classify_categorical(self) -> None:
        """name and department should be categorical."""
        assert "name" in self.col_types["categorical"]
        assert "department" in self.col_types["categorical"]

    def test_analysis_has_required_keys(self) -> None:
        """Analysis dict must contain all required top-level keys."""
        required = {
            "shape",
            "column_types",
            "missing_values",
            "high_missing_columns",
            "duplicate_rows",
            "numeric_statistics",
            "categorical_statistics",
            "correlation_matrix",
        }
        assert required.issubset(self.analysis.keys())

    def test_shape_correct(self) -> None:
        """Shape must match actual DataFrame dimensions."""
        assert self.analysis["shape"]["rows"] == 10
        assert self.analysis["shape"]["columns"] == 6

    def test_missing_values_structure(self) -> None:
        """Each column entry must have 'count' and 'percentage' keys."""
        for col, info in self.analysis["missing_values"].items():
            assert "count" in info
            assert "percentage" in info

    def test_age_has_one_missing(self) -> None:
        """Row 8 has no age value; should be counted."""
        assert self.analysis["missing_values"]["age"]["count"] == 1

    def test_numeric_stats_keys(self) -> None:
        """Numeric stats for 'salary' must contain standard stat keys."""
        salary_stats = self.analysis["numeric_statistics"].get("salary", {})
        for key in ("mean", "median", "std", "min", "max", "p25", "p75", "p95", "count"):
            assert key in salary_stats

    def test_categorical_stats_top10(self) -> None:
        """Categorical stats for 'department' must include top_10."""
        dept_stats = self.analysis["categorical_statistics"].get("department", {})
        assert "top_10" in dept_stats
        assert "unique_count" in dept_stats

    def test_correlation_matrix_present(self) -> None:
        """Correlation matrix should exist when >1 numeric column."""
        assert len(self.analysis["correlation_matrix"]) > 0

    def test_duplicate_rows_zero(self) -> None:
        """Fixture has no duplicate rows."""
        assert self.analysis["duplicate_rows"] == 0

    def test_high_missing_flag(self, tmp_path: Path) -> None:
        """Column with >20% missing should appear in high_missing_columns."""
        csv_path = tmp_path / "sparse.csv"
        # 10 rows, 8 missing in 'sparse_col' = 80% missing
        csv_path.write_text(
            "a,sparse_col\n1,\n2,\n3,\n4,\n5,\n6,\n7,\n8,\n9,10\n10,20\n",
            encoding="utf-8",
        )
        df2 = load_csv(str(csv_path))
        ct2 = classify_columns(df2)
        an2 = analyze_dataframe(df2, ct2)
        assert "sparse_col" in an2["high_missing_columns"]


# ---------------------------------------------------------------------------
# Output format tests
# ---------------------------------------------------------------------------

class TestOutputFormats:
    """Tests for write_json, write_csv_summary, write_html."""

    @pytest.fixture(autouse=True)
    def _setup(self, sample_csv: Path, tmp_path: Path) -> None:
        self.df = load_csv(str(sample_csv))
        self.col_types = classify_columns(self.df)
        self.analysis = analyze_dataframe(self.df, self.col_types)
        self.tmp = tmp_path

    def test_write_json_creates_file(self) -> None:
        """write_json should create a .json file in output_dir."""
        out = write_json(self.analysis, self.tmp, "test")
        assert out.exists()
        assert out.suffix == ".json"

    def test_write_json_valid_structure(self) -> None:
        """Written JSON must be parseable and contain 'shape'."""
        out = write_json(self.analysis, self.tmp, "test")
        data = json.loads(out.read_text())
        assert "shape" in data
        assert data["shape"]["rows"] == 10

    def test_write_csv_creates_file(self) -> None:
        """write_csv_summary should create a .csv file in output_dir."""
        out = write_csv_summary(self.analysis, self.tmp, "test")
        assert out.exists()
        assert out.suffix == ".csv"

    def test_write_csv_has_header_row(self) -> None:
        """Summary CSV must contain a 'column' header."""
        out = write_csv_summary(self.analysis, self.tmp, "test")
        content = out.read_text()
        assert "column" in content

    def test_write_html_creates_file(self) -> None:
        """write_html should create an .html file in output_dir."""
        out = write_html(self.df, self.analysis, {}, self.tmp, "test", "Test Title")
        assert out.exists()
        assert out.suffix == ".html"

    def test_write_html_contains_title(self) -> None:
        """HTML report must contain the specified title."""
        out = write_html(self.df, self.analysis, {}, self.tmp, "test", "My Custom Title")
        content = out.read_text()
        assert "My Custom Title" in content

    def test_write_html_contains_table(self) -> None:
        """HTML report must contain at least one <table> element."""
        out = write_html(self.df, self.analysis, {}, self.tmp, "test", "Test")
        content = out.read_text()
        assert "<table>" in content


# ---------------------------------------------------------------------------
# process_file integration test
# ---------------------------------------------------------------------------

class TestProcessFile:
    """Integration tests for process_file()."""

    def test_process_file_all_formats(self, sample_csv: Path, tmp_path: Path) -> None:
        """process_file with fmt='all' should succeed and create 3 output files."""
        success, paths = process_file(
            filepath=str(sample_csv),
            output_dir=tmp_path,
            fmt="all",
            title="Test",
            no_charts=True,
        )
        assert success is True
        assert len(paths) == 3
        extensions = {p.suffix for p in paths}
        assert ".json" in extensions
        assert ".csv" in extensions
        assert ".html" in extensions

    def test_process_file_json_only(self, sample_csv: Path, tmp_path: Path) -> None:
        """process_file with fmt='json' should create exactly 1 output file."""
        success, paths = process_file(
            filepath=str(sample_csv),
            output_dir=tmp_path,
            fmt="json",
            title="Test",
            no_charts=True,
        )
        assert success is True
        assert len(paths) == 1
        assert paths[0].suffix == ".json"

    def test_process_missing_file(self, tmp_path: Path) -> None:
        """process_file on a non-existent file should return success=False."""
        success, paths = process_file(
            filepath=str(tmp_path / "missing.csv"),
            output_dir=tmp_path,
            fmt="json",
            title="Test",
            no_charts=True,
        )
        assert success is False
        assert paths == []
