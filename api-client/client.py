"""Production-quality REST API client library with retry logic and typed exceptions.

Provides APIClient, a reusable HTTP client that handles authentication,
exponential-backoff retries, rate-limit respect, cursor-based pagination,
and typed exception hierarchy.

Example:
    from client import APIClient, RateLimitError

    client = APIClient(
        base_url="https://api.example.com/v1",
        bearer_token="your-token",
    )
    response = client.get("/users", params={"limit": 50})
    for page in client.paginate("/items"):
        process(page)
"""

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, Iterator, List, Optional, Union

import requests
from requests import Response, Session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class APIError(Exception):
    """Base exception for all API client errors.

    Attributes:
        status_code: HTTP status code that triggered the error.
        message: Human-readable error message.
        response: Raw requests.Response object, if available.
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response: Optional[Response] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response = response

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(status_code={self.status_code}, message={self.message!r})"


class RateLimitError(APIError):
    """Raised when the API returns 429 Too Many Requests and retries are exhausted.

    Attributes:
        retry_after: Seconds to wait as specified by the server, if provided.
    """

    def __init__(
        self,
        message: str,
        status_code: int = 429,
        response: Optional[Response] = None,
        retry_after: Optional[float] = None,
    ) -> None:
        super().__init__(message, status_code, response)
        self.retry_after = retry_after


class AuthenticationError(APIError):
    """Raised when the API returns 401 Unauthorized."""


class NotFoundError(APIError):
    """Raised when the API returns 404 Not Found."""


class ServerError(APIError):
    """Raised when the API returns a 5xx server-side error."""


# ---------------------------------------------------------------------------
# Response dataclass
# ---------------------------------------------------------------------------

@dataclass
class APIResponse:
    """Wrapper around a parsed API response.

    Attributes:
        status_code: HTTP status code.
        headers: Response headers dict.
        data: Parsed response body (JSON decoded to dict/list, or raw text).
        request_id: The X-Request-ID sent with the original request.
    """

    status_code: int
    headers: Dict[str, str]
    data: Any
    request_id: str


# ---------------------------------------------------------------------------
# Retry configuration constants
# ---------------------------------------------------------------------------

_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


# ---------------------------------------------------------------------------
# APIClient
# ---------------------------------------------------------------------------

class APIClient:
    """Production-quality REST API client with retry logic and auth injection.

    Supports Bearer token auth, X-API-Key auth, automatic retries with
    exponential backoff, Retry-After header respect on 429, cursor-based
    pagination, and typed exception mapping.

    Args:
        base_url: Base URL for all API requests (e.g. "https://api.example.com/v1").
        api_key: Optional API key sent as the X-API-Key header.
        bearer_token: Optional bearer token sent as Authorization: Bearer <token>.
        timeout: Request timeout in seconds (default: 30).
        max_retries: Maximum number of retry attempts on retryable errors (default: 3).
        retry_backoff_factor: Multiplier for exponential backoff delays (default: 0.5).
            Delay = backoff_factor * (2 ** (attempt - 1)).

    Example:
        client = APIClient(
            base_url="https://api.example.com/v1",
            bearer_token="tok_abc123",
            max_retries=3,
        )
        resp = client.get("/users", params={"active": True})
        print(resp.data)
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_backoff_factor: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.bearer_token = bearer_token
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor
        self._session: Session = requests.Session()

    def _build_auth_headers(self) -> Dict[str, str]:
        """Construct authentication headers based on configured credentials.

        Returns:
            Dict of auth header(s) to merge into the request.
        """
        headers: Dict[str, str] = {}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        elif self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def _parse_retry_after(self, response: Response) -> Optional[float]:
        """Parse the Retry-After header value in seconds.

        Args:
            response: The HTTP response object.

        Returns:
            Float seconds to wait, or None if header is absent/unparseable.
        """
        retry_after = response.headers.get("Retry-After")
        if retry_after is None:
            return None
        try:
            return float(retry_after)
        except ValueError:
            # Could be an HTTP-date; for simplicity treat as 1 second
            logger.debug("Non-numeric Retry-After header: %s; defaulting to 1s", retry_after)
            return 1.0

    def _raise_for_status(self, response: Response) -> None:
        """Map HTTP error status codes to typed exceptions.

        Args:
            response: The HTTP response to inspect.

        Raises:
            AuthenticationError: On 401.
            NotFoundError: On 404.
            RateLimitError: On 429.
            ServerError: On 5xx.
            APIError: On any other 4xx.
        """
        code = response.status_code
        if code < 400:
            return

        try:
            error_body = response.json()
            msg = error_body.get("message") or error_body.get("error") or response.text
        except Exception:
            msg = response.text or f"HTTP {code}"

        if code == 401:
            raise AuthenticationError(msg, status_code=code, response=response)
        if code == 404:
            raise NotFoundError(msg, status_code=code, response=response)
        if code == 429:
            retry_after = self._parse_retry_after(response)
            raise RateLimitError(msg, status_code=code, response=response, retry_after=retry_after)
        if 500 <= code < 600:
            raise ServerError(msg, status_code=code, response=response)
        raise APIError(msg, status_code=code, response=response)

    def request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> APIResponse:
        """Execute an HTTP request with retry logic and auth injection.

        Automatically injects authentication headers and a unique X-Request-ID
        on every call. Retries on 429/5xx with exponential backoff, respecting
        Retry-After headers. Raises typed exceptions on terminal error states.

        Args:
            method: HTTP method string (GET, POST, PUT, PATCH, DELETE).
            path: URL path relative to base_url (leading slash optional).
            **kwargs: Additional keyword arguments forwarded to requests.Session.request().

        Returns:
            APIResponse with status_code, headers, data, and request_id.

        Raises:
            RateLimitError: When 429 is returned and retries are exhausted.
            AuthenticationError: On 401.
            NotFoundError: On 404.
            ServerError: On persistent 5xx.
            APIError: On other HTTP errors.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_id = str(uuid.uuid4())

        # Build headers, preserving any caller-supplied headers
        headers: Dict[str, str] = {
            "X-Request-ID": request_id,
            "Accept": "application/json",
        }
        headers.update(self._build_auth_headers())
        headers.update(kwargs.pop("headers", {}))

        last_exc: Optional[APIError] = None

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                delay = self.retry_backoff_factor * (2 ** (attempt - 1))
                logger.debug(
                    "Retry attempt %d/%d for %s %s — waiting %.2fs",
                    attempt,
                    self.max_retries,
                    method.upper(),
                    url,
                    delay,
                )
                time.sleep(delay)

            try:
                response = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    timeout=self.timeout,
                    **kwargs,
                )
            except requests.exceptions.Timeout as exc:
                logger.warning("Request timeout on attempt %d: %s", attempt + 1, exc)
                last_exc = APIError(f"Request timed out: {exc}")
                continue
            except requests.exceptions.ConnectionError as exc:
                logger.warning("Connection error on attempt %d: %s", attempt + 1, exc)
                last_exc = APIError(f"Connection error: {exc}")
                continue

            logger.debug(
                "Response: %d %s %s (request_id=%s)",
                response.status_code,
                method.upper(),
                url,
                request_id,
            )

            # Handle rate limit with Retry-After
            if response.status_code == 429:
                retry_after = self._parse_retry_after(response)
                if retry_after is not None and attempt < self.max_retries:
                    logger.info(
                        "Rate limited (429); waiting %.1fs before retry (attempt %d/%d)",
                        retry_after,
                        attempt + 1,
                        self.max_retries,
                    )
                    time.sleep(retry_after)
                    last_exc = RateLimitError(
                        "Rate limited",
                        status_code=429,
                        response=response,
                        retry_after=retry_after,
                    )
                    continue
                self._raise_for_status(response)  # exhausted retries — raise

            # Retry on server errors
            if response.status_code in _RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                logger.warning(
                    "Retryable error %d for %s %s (attempt %d/%d)",
                    response.status_code,
                    method.upper(),
                    url,
                    attempt + 1,
                    self.max_retries,
                )
                last_exc = ServerError(
                    f"Server error {response.status_code}",
                    status_code=response.status_code,
                    response=response,
                )
                continue

            # Raise typed exception for non-retryable errors
            self._raise_for_status(response)

            # Success — parse and return
            try:
                data: Any = response.json()
            except ValueError:
                data = response.text

            return APIResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                data=data,
                request_id=request_id,
            )

        # All retries exhausted
        if last_exc is not None:
            raise last_exc
        raise APIError("Request failed after all retries")

    # ---------------------------------------------------------------------------
    # Convenience methods
    # ---------------------------------------------------------------------------

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> APIResponse:
        """Perform a GET request.

        Args:
            path: URL path relative to base_url.
            params: Optional query string parameters.

        Returns:
            APIResponse instance.
        """
        return self.request("GET", path, params=params)

    def post(
        self,
        path: str,
        json: Optional[Any] = None,
    ) -> APIResponse:
        """Perform a POST request with an optional JSON body.

        Args:
            path: URL path relative to base_url.
            json: Data to serialize as the JSON request body.

        Returns:
            APIResponse instance.
        """
        return self.request("POST", path, json=json)

    def put(
        self,
        path: str,
        json: Optional[Any] = None,
    ) -> APIResponse:
        """Perform a PUT request with an optional JSON body.

        Args:
            path: URL path relative to base_url.
            json: Data to serialize as the JSON request body.

        Returns:
            APIResponse instance.
        """
        return self.request("PUT", path, json=json)

    def patch(
        self,
        path: str,
        json: Optional[Any] = None,
    ) -> APIResponse:
        """Perform a PATCH request with an optional JSON body.

        Args:
            path: URL path relative to base_url.
            json: Data to serialize as the JSON request body.

        Returns:
            APIResponse instance.
        """
        return self.request("PATCH", path, json=json)

    def delete(
        self,
        path: str,
    ) -> APIResponse:
        """Perform a DELETE request.

        Args:
            path: URL path relative to base_url.

        Returns:
            APIResponse instance.
        """
        return self.request("DELETE", path)

    # ---------------------------------------------------------------------------
    # Pagination
    # ---------------------------------------------------------------------------

    def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        page_key: str = "cursor",
        data_key: str = "data",
    ) -> Generator[Any, None, None]:
        """Cursor-based pagination generator.

        Repeatedly fetches pages, yielding each item from the data list.
        Stops when the response no longer contains a next-page cursor.

        Args:
            path: URL path relative to base_url.
            params: Initial query parameters (will be augmented with pagination key).
            page_key: Key in the response JSON that holds the next-page cursor
                (default: 'cursor').
            data_key: Key in the response JSON that holds the page's data list
                (default: 'data').

        Yields:
            Individual items from each page's data list.

        Example:
            for item in client.paginate("/users", page_key="next_cursor", data_key="users"):
                process(item)
        """
        current_params: Dict[str, Any] = dict(params or {})

        while True:
            response = self.get(path, params=current_params)
            payload = response.data

            if not isinstance(payload, dict):
                # If the response is a bare list, yield all items and stop
                if isinstance(payload, list):
                    yield from payload
                return

            items = payload.get(data_key, [])
            if isinstance(items, list):
                yield from items
            else:
                yield items

            next_cursor = payload.get(page_key)
            if not next_cursor:
                break

            current_params = dict(params or {})
            current_params[page_key] = next_cursor

    def close(self) -> None:
        """Close the underlying HTTP session and release resources."""
        self._session.close()

    def __enter__(self) -> "APIClient":
        """Support context manager protocol."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Close session on context manager exit."""
        self.close()
