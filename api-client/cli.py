"""CLI wrapper for APIClient — make REST API calls from the command line.

Usage:
    python cli.py request --method GET --url https://api.example.com/v1/users
    python cli.py request --method POST --url https://api.example.com/v1/items \
        --auth-key sk_xxx --data '{"name": "Widget"}'
    python cli.py paginate --url https://api.example.com/v1/items --bearer tok_abc
"""

import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from client import (
    APIClient,
    APIError,
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _print_json(data: Any) -> None:
    """Pretty-print data as JSON.

    Args:
        data: Any JSON-serializable value.
    """
    print(json.dumps(data, indent=2, default=str))


def _print_table(data: Any) -> None:
    """Print data as a table using tabulate if available, else fall back to JSON.

    Args:
        data: List of dicts or any JSON-serializable value.
    """
    try:
        from tabulate import tabulate

        if isinstance(data, list) and data and isinstance(data[0], dict):
            headers = list(data[0].keys())
            rows = [list(item.values()) for item in data]
            print(tabulate(rows, headers=headers, tablefmt="rounded_outline"))
            return
        if isinstance(data, dict):
            rows = list(data.items())
            print(tabulate(rows, headers=["Key", "Value"], tablefmt="rounded_outline"))
            return
    except ImportError:
        logger.debug("tabulate not installed; falling back to JSON output")

    _print_json(data)


def _split_base_and_path(url: str) -> tuple[str, str]:
    """Split a full URL into base URL and path.

    Args:
        url: Full URL string.

    Returns:
        Tuple of (base_url, path).
    """
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path or "/"
    if parsed.query:
        path += f"?{parsed.query}"
    return base, path


def _parse_headers(header_strings: Optional[List[str]]) -> Dict[str, str]:
    """Parse a list of 'Key: Value' header strings into a dict.

    Args:
        header_strings: List of strings formatted as 'Header-Name: value'.

    Returns:
        Dict of header name to value.

    Raises:
        SystemExit: If any header string is malformed.
    """
    headers: Dict[str, str] = {}
    for hstr in (header_strings or []):
        if ": " not in hstr:
            print(f"Error: malformed --header value (expected 'Key: Value'): {hstr!r}",
                  file=sys.stderr)
            sys.exit(1)
        key, _, value = hstr.partition(": ")
        headers[key.strip()] = value.strip()
    return headers


# ---------------------------------------------------------------------------
# Sub-command handlers
# ---------------------------------------------------------------------------

def cmd_request(args: argparse.Namespace) -> int:
    """Handle the 'request' sub-command.

    Args:
        args: Parsed argument namespace.

    Returns:
        Exit code.
    """
    base_url, path = _split_base_and_path(args.url)
    extra_headers = _parse_headers(args.headers)

    body: Any = None
    if args.data:
        try:
            body = json.loads(args.data)
        except json.JSONDecodeError as exc:
            print(f"Error: --data is not valid JSON: {exc}", file=sys.stderr)
            return 1

    params: Optional[Dict[str, str]] = None
    if args.params:
        params = {}
        for pstr in args.params:
            if "=" not in pstr:
                print(f"Error: malformed --param (expected 'key=value'): {pstr!r}",
                      file=sys.stderr)
                return 1
            k, _, v = pstr.partition("=")
            params[k] = v

    client = APIClient(
        base_url=base_url,
        api_key=args.auth_key,
        bearer_token=args.bearer,
        timeout=args.timeout,
        max_retries=args.retries,
    )

    try:
        response = client.request(
            method=args.method.upper(),
            path=path,
            json=body,
            params=params,
            headers=extra_headers,
        )
    except AuthenticationError as exc:
        print(f"Authentication error ({exc.status_code}): {exc.message}", file=sys.stderr)
        return 2
    except NotFoundError as exc:
        print(f"Not found ({exc.status_code}): {exc.message}", file=sys.stderr)
        return 3
    except RateLimitError as exc:
        print(f"Rate limited ({exc.status_code}): {exc.message}", file=sys.stderr)
        return 4
    except ServerError as exc:
        print(f"Server error ({exc.status_code}): {exc.message}", file=sys.stderr)
        return 5
    except APIError as exc:
        print(f"API error ({exc.status_code}): {exc.message}", file=sys.stderr)
        return 6
    finally:
        client.close()

    if args.output == "table":
        _print_table(response.data)
    else:
        _print_json(response.data)

    return 0


def cmd_paginate(args: argparse.Namespace) -> int:
    """Handle the 'paginate' sub-command.

    Args:
        args: Parsed argument namespace.

    Returns:
        Exit code.
    """
    base_url, path = _split_base_and_path(args.url)

    params: Optional[Dict[str, str]] = None
    if args.params:
        params = {}
        for pstr in args.params:
            if "=" not in pstr:
                print(f"Error: malformed --param: {pstr!r}", file=sys.stderr)
                return 1
            k, _, v = pstr.partition("=")
            params[k] = v

    client = APIClient(
        base_url=base_url,
        api_key=args.auth_key,
        bearer_token=args.bearer,
        timeout=args.timeout,
        max_retries=args.retries,
    )

    all_items: List[Any] = []
    try:
        for item in client.paginate(
            path=path,
            params=params,
            page_key=args.page_key,
            data_key=args.data_key,
        ):
            all_items.append(item)
    except APIError as exc:
        print(f"API error ({exc.status_code}): {exc.message}", file=sys.stderr)
        return 6
    finally:
        client.close()

    if args.output == "table":
        _print_table(all_items)
    else:
        _print_json(all_items)

    print(f"\n# Total items fetched: {len(all_items)}", file=sys.stderr)
    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add shared auth/output/connection arguments to a sub-command parser.

    Args:
        parser: Sub-command ArgumentParser to augment.
    """
    auth = parser.add_argument_group("authentication (use one)")
    auth.add_argument("--auth-key", metavar="KEY", help="X-API-Key header value.")
    auth.add_argument("--bearer", metavar="TOKEN", help="Bearer token (Authorization header).")

    conn = parser.add_argument_group("connection")
    conn.add_argument("--timeout", type=int, default=30, metavar="SECS",
                      help="Request timeout in seconds (default: 30).")
    conn.add_argument("--retries", type=int, default=3, metavar="N",
                      help="Max retry attempts on retryable errors (default: 3).")

    parser.add_argument(
        "--output",
        choices=["json", "table"],
        default="json",
        help="Output format (default: json). 'table' requires tabulate.",
    )
    parser.add_argument(
        "--headers",
        action="append",
        metavar="'Key: Value'",
        help="Extra request header (repeatable): --headers 'X-Trace-ID: abc'",
    )
    parser.add_argument(
        "--params",
        action="append",
        metavar="key=value",
        help="Query parameter (repeatable): --params limit=50",
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser.

    Returns:
        Configured ArgumentParser.
    """
    parser = argparse.ArgumentParser(
        prog="api-client",
        description="Make REST API calls from the command line.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py request --method GET --url https://api.example.com/v1/users
  python cli.py request --method POST --url https://api.example.com/v1/items \\
      --auth-key sk_xxx --data '{"name": "Widget"}'
  python cli.py paginate --url https://api.example.com/v1/items --bearer tok_abc \\
      --page-key next_cursor --data-key results --output table
""",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging."
    )

    sub = parser.add_subparsers(dest="command", required=True, title="commands")

    # --- request ---
    req_parser = sub.add_parser("request", help="Make a single API request.")
    req_parser.add_argument(
        "--method", "-X",
        default="GET",
        metavar="METHOD",
        help="HTTP method (default: GET).",
    )
    req_parser.add_argument(
        "--url", "-u",
        required=True,
        metavar="URL",
        help="Full request URL.",
    )
    req_parser.add_argument(
        "--data", "-d",
        metavar="JSON",
        help="JSON body for POST/PUT/PATCH requests.",
    )
    _add_common_args(req_parser)

    # --- paginate ---
    pag_parser = sub.add_parser("paginate", help="Fetch all pages via cursor pagination.")
    pag_parser.add_argument(
        "--url", "-u",
        required=True,
        metavar="URL",
        help="Full URL of the paginated endpoint.",
    )
    pag_parser.add_argument(
        "--page-key",
        default="cursor",
        metavar="KEY",
        help="Response key containing the next-page cursor (default: cursor).",
    )
    pag_parser.add_argument(
        "--data-key",
        default="data",
        metavar="KEY",
        help="Response key containing the page's data list (default: data).",
    )
    _add_common_args(pag_parser)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.command == "request":
        return cmd_request(args)
    if args.command == "paginate":
        return cmd_paginate(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
