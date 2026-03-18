# REST API Client

A reusable, production-quality REST API client library with automatic retry logic, typed exceptions, cursor-based pagination, and a CLI wrapper. Demonstrates: `requests`, exponential backoff, auth injection, `dataclasses`, and full type hints.

## Features

- **Automatic auth injection** — Bearer token or X-API-Key
- **Retry with exponential backoff** — on 429, 500, 502, 503, 504
- **Retry-After header respect** — waits the server-specified delay on 429
- **Unique X-Request-ID** on every request (UUID4)
- **Typed exception hierarchy** — `APIError` → `RateLimitError`, `AuthenticationError`, `NotFoundError`, `ServerError`
- **Cursor-based pagination generator** — automatically fetches all pages
- **`@dataclass` response wrapper** — typed, consistent response object
- **Context manager support** — clean session lifecycle
- **CLI** — make API calls directly from the terminal

## Installation

```bash
cd api-client
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Or use the setup script (also runs tests):

```bash
bash setup.sh
```

## Usage as a Library

### Basic requests

```python
from client import APIClient

client = APIClient(
    base_url="https://api.example.com/v1",
    bearer_token="your-token",
)

# GET
response = client.get("/users", params={"limit": 50, "active": True})
print(response.data)          # parsed JSON
print(response.status_code)   # 200
print(response.request_id)    # UUID sent as X-Request-ID

# POST
response = client.post("/users", json={"name": "Alice", "email": "alice@example.com"})

# PUT / PATCH / DELETE
client.put("/users/42", json={"name": "Alice Smith"})
client.patch("/users/42", json={"email": "new@example.com"})
client.delete("/users/42")
```

### Cursor-based pagination

```python
# Yields individual items across all pages automatically
for user in client.paginate("/users", page_key="cursor", data_key="data"):
    print(user["id"], user["name"])

# Custom envelope keys
for result in client.paginate(
    "/search/results",
    params={"q": "python"},
    page_key="next_token",
    data_key="results",
):
    print(result)
```

### Exception handling

```python
from client import (
    APIClient,
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
    APIError,
)

client = APIClient(base_url="https://api.example.com/v1", api_key="sk_xxx")

try:
    response = client.get("/users/999")
except AuthenticationError:
    print("Invalid API key — check credentials")
except NotFoundError:
    print("User not found")
except RateLimitError as e:
    print(f"Rate limited; retry after {e.retry_after}s")
except ServerError as e:
    print(f"Server error {e.status_code}: {e.message}")
except APIError as e:
    print(f"Unexpected error: {e.message}")
```

### Context manager

```python
with APIClient(base_url="https://api.example.com/v1", bearer_token="tok") as client:
    data = client.get("/items").data
# Session is automatically closed
```

### Custom configuration

```python
client = APIClient(
    base_url="https://api.example.com/v1",
    bearer_token="your-token",
    timeout=60,            # longer timeout for slow endpoints
    max_retries=5,         # more retries
    retry_backoff_factor=1.0,  # longer backoff delays
)
```

## CLI Usage

### Single request

```bash
# GET request
python cli.py request --method GET --url https://api.example.com/v1/users

# GET with auth and query params
python cli.py request \
    --method GET \
    --url https://api.example.com/v1/users \
    --auth-key sk_xxx \
    --params limit=50 \
    --params active=true

# POST with JSON body
python cli.py request \
    --method POST \
    --url https://api.example.com/v1/users \
    --bearer your-token \
    --data '{"name": "Alice", "email": "alice@example.com"}'

# With extra headers
python cli.py request \
    --method GET \
    --url https://api.example.com/v1/items \
    --headers 'X-Trace-ID: abc123' \
    --headers 'Accept-Language: en'

# Table output (requires tabulate)
python cli.py request --method GET --url https://api.example.com/v1/users --output table
```

### Paginated fetch

```bash
# Fetch all pages, print as JSON array
python cli.py paginate \
    --url https://api.example.com/v1/items \
    --auth-key sk_xxx

# Custom pagination keys
python cli.py paginate \
    --url https://api.example.com/v1/results \
    --bearer your-token \
    --page-key next_token \
    --data-key results \
    --output table
```

### Global options

| Flag | Description |
|------|-------------|
| `-v / --verbose` | Enable debug logging |
| `--auth-key KEY` | X-API-Key header value |
| `--bearer TOKEN` | Bearer token |
| `--timeout SECS` | Request timeout (default: 30) |
| `--retries N` | Max retries (default: 3) |
| `--output json\|table` | Output format (default: json) |
| `--headers 'K: V'` | Extra headers (repeatable) |
| `--params key=val` | Query parameters (repeatable) |

## Running Tests

```bash
source .venv/bin/activate
pytest tests/ -v
```

All tests use the `responses` library to mock HTTP — no real network calls.

## Exception Reference

| Exception | HTTP Status | Description |
|-----------|------------|-------------|
| `APIError` | any 4xx/5xx | Base class for all API errors |
| `AuthenticationError` | 401 | Invalid or missing credentials |
| `NotFoundError` | 404 | Resource does not exist |
| `RateLimitError` | 429 | Too many requests (includes `retry_after`) |
| `ServerError` | 5xx | Server-side error |
