# CSV Report Pipeline

A CLI tool that reads one or more CSV files and generates structured analysis reports in JSON, CSV, and HTML formats. Demonstrates pandas, argparse, data validation, and matplotlib chart generation.

## Features

- **Automatic encoding detection** — tries UTF-8, falls back to latin-1
- **Column type classification** — numeric, categorical, datetime
- **Missing value analysis** — counts, percentages, flags columns >20% missing
- **Summary statistics** — mean, median, std, min, max, percentiles for numeric columns
- **Value counts** — top-10 for categorical columns
- **Correlation matrix** — for multiple numeric columns
- **Duplicate row detection**
- **Multiple output formats** — JSON, CSV summary, styled HTML with embedded charts
- **Proper logging** — structured log output, no stray prints

## Installation

```bash
cd csv-reporter
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Or use the setup script (also runs tests):

```bash
bash setup.sh
```

## Usage

```bash
# Basic — analyze one file, output all formats to ./reports/
python reporter.py data.csv

# Multiple files
python reporter.py sales_q1.csv sales_q2.csv --title "Sales Analysis 2024"

# HTML only, custom output directory
python reporter.py data.csv --format html --output-dir ./output

# JSON only, verbose logging
python reporter.py data.csv --format json -v

# Skip chart generation (faster)
python reporter.py data.csv --no-charts

# Full options
python reporter.py data.csv \
    --output-dir ./reports \
    --format all \
    --title "Q4 Report" \
    --verbose
```

## Output Formats

### JSON (`<stem>_report.json`)
Machine-readable full analysis:
```json
{
  "shape": {"rows": 1000, "columns": 8},
  "column_types": {
    "numeric": ["age", "salary"],
    "categorical": ["department", "name"],
    "datetime": ["hire_date"]
  },
  "missing_values": {
    "age": {"count": 5, "percentage": 0.5}
  },
  "numeric_statistics": {
    "salary": {
      "mean": 72000.5,
      "median": 70000.0,
      "std": 15234.2,
      "min": 40000.0,
      "max": 150000.0,
      "p25": 60000.0,
      "p75": 85000.0,
      "p95": 110000.0,
      "count": 995
    }
  },
  "categorical_statistics": {
    "department": {
      "unique_count": 5,
      "top_10": {"Engineering": 350, "Sales": 200}
    }
  },
  "correlation_matrix": {...},
  "duplicate_rows": 0,
  "high_missing_columns": []
}
```

### CSV (`<stem>_summary.csv`)
Flat table of numeric column statistics, easy to import into Excel or other tools:
```
column,mean,median,std,min,max,p25,p75,p95,count
salary,72000.5,70000.0,15234.2,40000.0,150000.0,60000.0,85000.0,110000.0,995
age,34.2,33.0,8.1,22.0,65.0,28.0,40.0,54.0,995
```

### HTML (`<stem>_report.html`)
Fully self-contained styled report with:
- Dataset overview (row/column counts, duplicate count)
- Column type breakdown
- Missing value table
- Per-column stat cards with inline histogram charts
- Categorical value count tables with bar charts
- Correlation matrix table

## Exit Codes

| Code | Meaning |
|------|---------|
| 0    | Success |
| 1    | Input error (file not found, unreadable) |
| 2    | Analysis error |

## Running Tests

```bash
source .venv/bin/activate
pytest tests/ -v
```
