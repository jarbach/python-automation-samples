"""CSV Report Pipeline — CLI tool for structured CSV analysis and reporting.

Reads one or more CSV files, performs statistical analysis, and outputs
reports in JSON, CSV, and/or HTML formats with optional inline charts.

Usage:
    python reporter.py data.csv --format all --output-dir ./reports
    python reporter.py a.csv b.csv --format html --title "Q4 Analysis"
"""

import argparse
import base64
import io
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data loading & validation
# ---------------------------------------------------------------------------

def detect_encoding(filepath: str) -> str:
    """Detect file encoding, falling back to latin-1 if UTF-8 fails.

    Args:
        filepath: Path to the file to inspect.

    Returns:
        Encoding string ('utf-8' or 'latin-1').
    """
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            fh.read(4096)
        return "utf-8"
    except UnicodeDecodeError:
        logger.debug("UTF-8 decode failed for %s; falling back to latin-1", filepath)
        return "latin-1"


def load_csv(filepath: str) -> pd.DataFrame:
    """Load a CSV file into a DataFrame with automatic encoding detection.

    Args:
        filepath: Absolute or relative path to the CSV file.

    Returns:
        Loaded DataFrame.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file cannot be parsed as CSV.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    if not path.is_file():
        raise ValueError(f"Path is not a file: {filepath}")

    encoding = detect_encoding(filepath)
    logger.debug("Loading %s with encoding=%s", filepath, encoding)
    try:
        df = pd.read_csv(filepath, encoding=encoding)
    except Exception as exc:
        raise ValueError(f"Failed to parse CSV '{filepath}': {exc}") from exc

    logger.info("Loaded %s — %d rows x %d columns", filepath, len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Column type classification
# ---------------------------------------------------------------------------

def classify_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """Classify DataFrame columns into numeric, categorical, and datetime groups.

    Args:
        df: Input DataFrame.

    Returns:
        Dict with keys 'numeric', 'categorical', 'datetime', each mapping to
        a list of column names.
    """
    numeric: List[str] = []
    categorical: List[str] = []
    datetime_cols: List[str] = []

    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_numeric_dtype(dtype):
            numeric.append(col)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            datetime_cols.append(col)
        else:
            try:
                parsed = pd.to_datetime(df[col], errors="coerce")
                non_null_ratio = parsed.notna().sum() / max(len(df), 1)
                if non_null_ratio >= 0.8:
                    datetime_cols.append(col)
                else:
                    categorical.append(col)
            except Exception:
                categorical.append(col)

    return {"numeric": numeric, "categorical": categorical, "datetime": datetime_cols}


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyze_dataframe(df: pd.DataFrame, column_types: Dict[str, List[str]]) -> Dict[str, Any]:
    """Run full statistical analysis on a DataFrame.

    Computes missing value counts, duplicate detection, summary statistics for
    numeric columns, value counts for categorical columns, and correlation matrix.

    Args:
        df: Input DataFrame.
        column_types: Column classification from classify_columns().

    Returns:
        Nested dict containing all analysis results.
    """
    total_rows = len(df)
    total_cols = len(df.columns)

    # Missing values
    missing: Dict[str, Any] = {}
    high_missing_cols: List[str] = []
    for col in df.columns:
        count = int(df[col].isna().sum())
        pct = round(count / max(total_rows, 1) * 100, 2)
        missing[col] = {"count": count, "percentage": pct}
        if pct > 20:
            high_missing_cols.append(col)

    if high_missing_cols:
        logger.warning("Columns with >20%% missing values: %s", high_missing_cols)

    # Duplicates
    duplicate_count = int(df.duplicated().sum())

    # Numeric stats
    numeric_stats: Dict[str, Any] = {}
    for col in column_types["numeric"]:
        series = df[col].dropna()
        if series.empty:
            numeric_stats[col] = {}
            continue
        numeric_stats[col] = {
            "mean": round(float(series.mean()), 6),
            "median": round(float(series.median()), 6),
            "std": round(float(series.std()), 6),
            "min": round(float(series.min()), 6),
            "max": round(float(series.max()), 6),
            "p25": round(float(series.quantile(0.25)), 6),
            "p75": round(float(series.quantile(0.75)), 6),
            "p95": round(float(series.quantile(0.95)), 6),
            "count": int(series.count()),
        }

    # Categorical value counts
    categorical_stats: Dict[str, Any] = {}
    for col in column_types["categorical"]:
        vc = df[col].value_counts(dropna=False).head(10)
        categorical_stats[col] = {
            "unique_count": int(df[col].nunique(dropna=True)),
            "top_10": {str(k): int(v) for k, v in vc.items()},
        }

    # Correlation matrix
    correlation: Dict[str, Any] = {}
    if len(column_types["numeric"]) > 1:
        corr_df = df[column_types["numeric"]].corr()
        correlation = {
            col: {other: round(float(val), 6) for other, val in row.items()}
            for col, row in corr_df.to_dict().items()
        }

    return {
        "shape": {"rows": total_rows, "columns": total_cols},
        "column_types": column_types,
        "missing_values": missing,
        "high_missing_columns": high_missing_cols,
        "duplicate_rows": duplicate_count,
        "numeric_statistics": numeric_stats,
        "categorical_statistics": categorical_stats,
        "correlation_matrix": correlation,
    }


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------

def _fig_to_b64(fig: Any) -> str:
    """Serialize a matplotlib figure to a base64-encoded PNG string.

    Args:
        fig: A matplotlib Figure object.

    Returns:
        Base64-encoded PNG as a UTF-8 string.
    """
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_charts(
    df: pd.DataFrame,
    column_types: Dict[str, List[str]],
) -> Dict[str, str]:
    """Generate base64-encoded PNG charts for numeric and categorical columns.

    Args:
        df: Input DataFrame.
        column_types: Column classification from classify_columns().

    Returns:
        Dict mapping column name to base64 PNG string.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not available; skipping charts")
        return {}

    charts: Dict[str, str] = {}

    for col in column_types["numeric"]:
        series = df[col].dropna()
        if series.empty:
            continue
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.hist(series, bins=30, edgecolor="black", color="#4C72B0")
        ax.set_title(f"Distribution: {col}")
        ax.set_xlabel(col)
        ax.set_ylabel("Frequency")
        charts[col] = _fig_to_b64(fig)
        plt.close(fig)

    for col in column_types["categorical"]:
        vc = df[col].value_counts(dropna=False).head(10)
        if vc.empty:
            continue
        fig, ax = plt.subplots(figsize=(7, 3))
        vc.plot(kind="barh", ax=ax, color="#55A868")
        ax.set_title(f"Top Values: {col}")
        ax.set_xlabel("Count")
        ax.invert_yaxis()
        charts[col] = _fig_to_b64(fig)
        plt.close(fig)

    return charts


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_json(
    analysis: Dict[str, Any],
    output_dir: Path,
    stem: str,
) -> Path:
    """Write analysis results to a JSON file.

    Args:
        analysis: Analysis dict from analyze_dataframe().
        output_dir: Directory to write the output file.
        stem: Base name (without extension) for the output file.

    Returns:
        Path of the written file.
    """
    out_path = output_dir / f"{stem}_report.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(analysis, fh, indent=2, default=str)
    logger.info("JSON report written to %s", out_path)
    return out_path


def write_csv_summary(
    analysis: Dict[str, Any],
    output_dir: Path,
    stem: str,
) -> Path:
    """Write numeric summary statistics to a CSV file.

    Args:
        analysis: Analysis dict from analyze_dataframe().
        output_dir: Directory to write the output file.
        stem: Base name (without extension) for the output file.

    Returns:
        Path of the written file.
    """
    rows = []
    for col, stats in analysis.get("numeric_statistics", {}).items():
        if stats:
            row = {"column": col}
            row.update(stats)
            rows.append(row)

    out_path = output_dir / f"{stem}_summary.csv"
    if rows:
        pd.DataFrame(rows).to_csv(out_path, index=False)
    else:
        pd.DataFrame(columns=["column"]).to_csv(out_path, index=False)

    logger.info("CSV summary written to %s", out_path)
    return out_path


_HTML_STYLE = """
<style>
  body { font-family: Arial, sans-serif; margin: 40px; color: #222; }
  h1 { color: #2c3e50; }
  h2 { color: #34495e; border-bottom: 2px solid #ecf0f1; padding-bottom: 4px; }
  h3 { color: #555; }
  table { border-collapse: collapse; width: 100%; margin-bottom: 20px; font-size: 0.9em; }
  th { background-color: #2c3e50; color: white; padding: 8px 12px; text-align: left; }
  td { padding: 6px 12px; border-bottom: 1px solid #ddd; }
  tr:nth-child(even) { background-color: #f9f9f9; }
  .warning { background: #fff3cd; border-left: 4px solid #ffc107; padding: 8px 12px; margin: 10px 0; }
  .stat-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; }
  .stat-card { border: 1px solid #ddd; border-radius: 6px; padding: 12px; background: #fafafa; }
  img { max-width: 100%; border: 1px solid #eee; border-radius: 4px; margin-top: 8px; }
</style>
"""


def write_html(
    df: pd.DataFrame,
    analysis: Dict[str, Any],
    charts: Dict[str, str],
    output_dir: Path,
    stem: str,
    title: str,
) -> Path:
    """Write a styled HTML report with embedded base64-encoded charts.

    Args:
        df: Original DataFrame (for metadata display).
        analysis: Analysis dict from analyze_dataframe().
        charts: Dict of column to base64 PNG from generate_charts().
        output_dir: Directory to write the output file.
        stem: Base name (without extension) for the output file.
        title: Report title string.

    Returns:
        Path of the written file.
    """
    shape = analysis["shape"]
    missing = analysis["missing_values"]
    high_miss = analysis["high_missing_columns"]
    num_stats = analysis["numeric_statistics"]
    cat_stats = analysis["categorical_statistics"]
    corr = analysis["correlation_matrix"]
    dup_count = analysis["duplicate_rows"]
    col_types = analysis["column_types"]

    parts: List[str] = [
        "<!DOCTYPE html><html><head>",
        f"<meta charset='utf-8'><title>{title}</title>",
        _HTML_STYLE,
        "</head><body>",
        f"<h1>{title}</h1>",
        f"<p><strong>Source:</strong> {stem} &nbsp;|&nbsp; "
        f"<strong>Rows:</strong> {shape['rows']} &nbsp;|&nbsp; "
        f"<strong>Columns:</strong> {shape['columns']} &nbsp;|&nbsp; "
        f"<strong>Duplicate rows:</strong> {dup_count}</p>",
    ]

    parts.append("<h2>Column Types</h2><ul>")
    for kind, cols in col_types.items():
        if cols:
            parts.append(f"<li><strong>{kind.capitalize()}:</strong> {', '.join(cols)}</li>")
    parts.append("</ul>")

    if high_miss:
        parts.append(
            f"<div class='warning'>Warning: Columns with &gt;20% missing values: "
            f"<strong>{', '.join(high_miss)}</strong></div>"
        )

    parts.append("<h2>Missing Values</h2>")
    parts.append("<table><tr><th>Column</th><th>Missing Count</th><th>Missing %</th></tr>")
    for col, info in missing.items():
        parts.append(
            f"<tr><td>{col}</td><td>{info['count']}</td><td>{info['percentage']}%</td></tr>"
        )
    parts.append("</table>")

    if num_stats:
        parts.append("<h2>Numeric Column Statistics</h2><div class='stat-grid'>")
        for col, stats in num_stats.items():
            if not stats:
                continue
            parts.append(f"<div class='stat-card'><h3>{col}</h3><table>")
            for k, v in stats.items():
                parts.append(f"<tr><td>{k}</td><td>{v}</td></tr>")
            parts.append("</table>")
            if col in charts:
                parts.append(f"<img src='data:image/png;base64,{charts[col]}' alt='{col} chart'>")
            parts.append("</div>")
        parts.append("</div>")

    if cat_stats:
        parts.append("<h2>Categorical Column Statistics</h2>")
        for col, stats in cat_stats.items():
            parts.append(
                f"<h3>{col} <small>({stats['unique_count']} unique values)</small></h3>"
            )
            parts.append("<table><tr><th>Value</th><th>Count</th></tr>")
            for val, cnt in stats["top_10"].items():
                parts.append(f"<tr><td>{val}</td><td>{cnt}</td></tr>")
            parts.append("</table>")
            if col in charts:
                parts.append(f"<img src='data:image/png;base64,{charts[col]}' alt='{col} chart'>")

    if corr:
        cols_order = list(corr.keys())
        parts.append("<h2>Correlation Matrix</h2><table>")
        parts.append("<tr><th></th>" + "".join(f"<th>{c}</th>" for c in cols_order) + "</tr>")
        for row_col in cols_order:
            parts.append(f"<tr><td><strong>{row_col}</strong></td>")
            for other_col in cols_order:
                val = corr[row_col].get(other_col, "")
                parts.append(f"<td>{val}</td>")
            parts.append("</tr>")
        parts.append("</table>")

    parts.append("</body></html>")

    out_path = output_dir / f"{stem}_report.html"
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(parts))
    logger.info("HTML report written to %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------------

def process_file(
    filepath: str,
    output_dir: Path,
    fmt: str,
    title: str,
    no_charts: bool,
) -> Tuple[bool, List[Path]]:
    """Run the full analysis pipeline for a single CSV file.

    Args:
        filepath: Path to the input CSV file.
        output_dir: Directory for output files.
        fmt: Output format — 'json', 'csv', 'html', or 'all'.
        title: Report title string.
        no_charts: If True, skip chart generation.

    Returns:
        Tuple of (success: bool, list of output paths).
    """
    stem = Path(filepath).stem
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths: List[Path] = []

    try:
        df = load_csv(filepath)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Input error for %s: %s", filepath, exc)
        return False, []

    try:
        column_types = classify_columns(df)
        logger.debug("Column types: %s", column_types)

        analysis = analyze_dataframe(df, column_types)
        analysis["source_file"] = filepath
        analysis["title"] = title

        charts: Dict[str, str] = {}
        if not no_charts and fmt in ("html", "all"):
            charts = generate_charts(df, column_types)

        if fmt in ("json", "all"):
            output_paths.append(write_json(analysis, output_dir, stem))
        if fmt in ("csv", "all"):
            output_paths.append(write_csv_summary(analysis, output_dir, stem))
        if fmt in ("html", "all"):
            output_paths.append(write_html(df, analysis, charts, output_dir, stem, title))

    except Exception as exc:
        logger.error("Analysis error for %s: %s", filepath, exc)
        return False, output_paths

    return True, output_paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the CLI.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="reporter",
        description="Generate structured analysis reports from CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python reporter.py data.csv
  python reporter.py a.csv b.csv --format html --title "Sales Q4"
  python reporter.py data.csv --output-dir ./out --format json -v
""",
    )
    parser.add_argument(
        "input",
        nargs="+",
        metavar="CSV_FILE",
        help="One or more CSV file paths to analyze.",
    )
    parser.add_argument(
        "--output-dir",
        default="./reports",
        metavar="DIR",
        help="Directory for output files (default: ./reports).",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv", "html", "all"],
        default="all",
        dest="format",
        help="Output format (default: all).",
    )
    parser.add_argument(
        "--title",
        default="Data Analysis Report",
        help='Report title (default: "Data Analysis Report").',
    )
    parser.add_argument(
        "--no-charts",
        action="store_true",
        help="Skip chart generation (speeds up processing).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose/debug logging.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point for the CSV reporter CLI.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code: 0 = success, 1 = input error, 2 = analysis error.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    output_dir = Path(args.output_dir)
    any_input_error = False
    any_analysis_error = False

    for filepath in args.input:
        logger.info("Processing: %s", filepath)
        success, paths = process_file(
            filepath=filepath,
            output_dir=output_dir,
            fmt=args.format,
            title=args.title,
            no_charts=args.no_charts,
        )
        if not success:
            if not Path(filepath).exists():
                any_input_error = True
            else:
                any_analysis_error = True
        else:
            for p in paths:
                logger.info("  -> %s", p)

    if any_input_error:
        return 1
    if any_analysis_error:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
