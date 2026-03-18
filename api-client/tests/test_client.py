"""Tests for client.py — REST API client library.

Uses the 'responses' library to mock HTTP calls so no real network is required.
"""

import sys
import os
import time
from typing import Any, Dict, List
from unittest.mock import patch

import pytest
import responses as rsps_lib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from client import (
    APIClient,
    APIError,
    APIResponse,
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
)


BASE_URL = "https://api.example.com/v1"


@pytest.fixture()
def client() -> APIClient:
    """Provide an APIClient with a short backoff for faster tests."""
    return APIClient(
        base_url=BASE_URL,
        bearer_token="test-token",
        max_retries=3,
        retry_backoff_factor=0.0,  # zero delay for fast tests
    )


# ---------------------------------------------------------------------------
# Successful GET
# ---------------------------------------------------------------------------

class TestSuccessfulGet:
    """Tests for happy-path GET requests."""

    @rsps_lib.activate
    def test_successful_get(self, client: APIClient) -> None:
        """Should return APIResponse with correct data on 200."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/users",
            json={"users": [{"id": 1, "name": "Alice"}]},
            status=200,
        )
        resp = client.get("/users")
        assert isinstance(resp, APIResponse)
        assert resp.status_code == 200
        assert resp.data["users"][0]["name"] == "Alice"

    @rsps_lib.activate
    def test_get_with_params(self, client: APIClient) -> None:
        """Should pass query parameters correctly."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/users",
            json={"users": []},
            status=200,
        )
        resp = client.get("/users", params={"active": "true", "limit": "10"})
        assert resp.status_code == 200
        # Verify params were sent
        assert "active=true" in rsps_lib.calls[0].request.url

    @rsps_lib.activate
    def test_request_id_header_set(self, client: APIClient) -> None:
        """Every request must include an X-Request-ID header."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/ping",
            json={"ok": True},
            status=200,
        )
        client.get("/ping")
        sent_headers = rsps_lib.calls[0].request.headers
        assert "X-Request-ID" in sent_headers
        # UUID4 format check (8-4-4-4-12 characters)
        request_id = sent_headers["X-Request-ID"]
        parts = request_id.split("-")
        assert len(parts) == 5

    @rsps_lib.activate
    def test_request_id_unique_per_request(self, client: APIClient) -> None:
        """Each request should get its own unique X-Request-ID."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/ping",
            json={"ok": True},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/ping",
            json={"ok": True},
            status=200,
        )
        client.get("/ping")
        client.get("/ping")
        id1 = rsps_lib.calls[0].request.headers["X-Request-ID"]
        id2 = rsps_lib.calls[1].request.headers["X-Request-ID"]
        assert id1 != id2

    @rsps_lib.activate
    def test_bearer_token_injected(self, client: APIClient) -> None:
        """Bearer token should appear in Authorization header."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/me",
            json={"id": 1},
            status=200,
        )
        client.get("/me")
        auth_header = rsps_lib.calls[0].request.headers.get("Authorization", "")
        assert auth_header == "Bearer test-token"

    @rsps_lib.activate
    def test_api_key_injected(self) -> None:
        """X-API-Key should be set when api_key is provided."""
        key_client = APIClient(base_url=BASE_URL, api_key="sk_test_123")
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/items",
            json=[],
            status=200,
        )
        key_client.get("/items")
        assert rsps_lib.calls[0].request.headers.get("X-API-Key") == "sk_test_123"


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

class TestErrorMapping:
    """Tests for typed exception raising."""

    @rsps_lib.activate
    def test_401_raises_authentication_error(self, client: APIClient) -> None:
        """A 401 response should raise AuthenticationError."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/secure",
            json={"message": "Unauthorized"},
            status=401,
        )
        with pytest.raises(AuthenticationError) as exc_info:
            client.get("/secure")
        assert exc_info.value.status_code == 401

    @rsps_lib.activate
    def test_404_raises_not_found_error(self, client: APIClient) -> None:
        """A 404 response should raise NotFoundError."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/missing",
            json={"message": "Not found"},
            status=404,
        )
        with pytest.raises(NotFoundError) as exc_info:
            client.get("/missing")
        assert exc_info.value.status_code == 404

    @rsps_lib.activate
    def test_generic_4xx_raises_api_error(self, client: APIClient) -> None:
        """A 400 response should raise the base APIError."""
        rsps_lib.add(
            rsps_lib.POST,
            f"{BASE_URL}/items",
            json={"message": "Bad request"},
            status=400,
        )
        with pytest.raises(APIError) as exc_info:
            client.post("/items", json={"bad": "data"})
        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------

class TestRetryBehavior:
    """Tests for retry logic on 5xx and 429."""

    @rsps_lib.activate
    def test_retry_on_503(self, client: APIClient) -> None:
        """Should retry up to max_retries times on 503 before raising ServerError."""
        # 3 failures then 1 success (max_retries=3 means 4 total attempts)
        for _ in range(3):
            rsps_lib.add(
                rsps_lib.GET,
                f"{BASE_URL}/health",
                json={"error": "Service unavailable"},
                status=503,
            )
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/health",
            json={"status": "ok"},
            status=200,
        )
        resp = client.get("/health")
        assert resp.status_code == 200
        assert len(rsps_lib.calls) == 4

    @rsps_lib.activate
    def test_retry_exhausted_raises_server_error(self, client: APIClient) -> None:
        """Should raise ServerError when all retries are exhausted on 503."""
        for _ in range(4):  # max_retries=3 means 4 total attempts (0..3)
            rsps_lib.add(
                rsps_lib.GET,
                f"{BASE_URL}/health",
                json={"error": "down"},
                status=503,
            )
        with pytest.raises(ServerError):
            client.get("/health")
        assert len(rsps_lib.calls) == 4

    @rsps_lib.activate
    def test_429_with_retry_after_waits_and_retries(self, client: APIClient) -> None:
        """429 with Retry-After header should wait and then retry successfully."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/limited",
            json={"error": "rate limited"},
            status=429,
            headers={"Retry-After": "0"},  # 0 seconds for test speed
        )
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/limited",
            json={"data": "ok"},
            status=200,
        )

        sleep_calls: List[float] = []
        original_sleep = time.sleep

        with patch("client.time.sleep", side_effect=lambda x: sleep_calls.append(x)):
            resp = client.get("/limited")

        assert resp.status_code == 200
        assert len(rsps_lib.calls) == 2
        # Should have slept for the Retry-After value
        assert any(s == 0.0 for s in sleep_calls)

    @rsps_lib.activate
    def test_429_exhausted_raises_rate_limit_error(self) -> None:
        """Should raise RateLimitError when 429 retries are exhausted."""
        single_retry_client = APIClient(
            base_url=BASE_URL,
            bearer_token="tok",
            max_retries=1,
            retry_backoff_factor=0.0,
        )
        for _ in range(3):
            rsps_lib.add(
                rsps_lib.GET,
                f"{BASE_URL}/limited",
                json={"error": "too many requests"},
                status=429,
                headers={"Retry-After": "0"},
            )
        with pytest.raises(RateLimitError):
            with patch("client.time.sleep"):
                single_retry_client.get("/limited")


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

class TestPagination:
    """Tests for the paginate() generator."""

    @rsps_lib.activate
    def test_paginate_yields_all_items(self, client: APIClient) -> None:
        """paginate() should yield items across multiple pages until cursor is None."""
        # Page 1
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/items",
            json={"data": [{"id": 1}, {"id": 2}], "cursor": "page2"},
            status=200,
        )
        # Page 2
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/items",
            json={"data": [{"id": 3}, {"id": 4}], "cursor": "page3"},
            status=200,
        )
        # Page 3 (last)
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/items",
            json={"data": [{"id": 5}], "cursor": None},
            status=200,
        )

        items = list(client.paginate("/items"))
        assert len(items) == 5
        assert items[0]["id"] == 1
        assert items[-1]["id"] == 5
        assert len(rsps_lib.calls) == 3

    @rsps_lib.activate
    def test_paginate_single_page(self, client: APIClient) -> None:
        """Single-page responses should stop pagination immediately."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/events",
            json={"data": [{"id": 10}, {"id": 11}]},
            status=200,
        )

        items = list(client.paginate("/events"))
        assert len(items) == 2
        assert len(rsps_lib.calls) == 1

    @rsps_lib.activate
    def test_paginate_custom_keys(self, client: APIClient) -> None:
        """paginate() should respect custom page_key and data_key arguments."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/results",
            json={"results": [{"val": "a"}], "next_token": "tok2"},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/results",
            json={"results": [{"val": "b"}], "next_token": None},
            status=200,
        )

        items = list(client.paginate("/results", page_key="next_token", data_key="results"))
        assert len(items) == 2
        assert items[0]["val"] == "a"
        assert items[1]["val"] == "b"

    @rsps_lib.activate
    def test_paginate_bare_list_response(self, client: APIClient) -> None:
        """paginate() should handle bare-list responses (no envelope)."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/flat",
            json=[{"id": 1}, {"id": 2}, {"id": 3}],
            status=200,
        )

        items = list(client.paginate("/flat"))
        assert len(items) == 3


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    """Tests for APIClient context manager protocol."""

    @rsps_lib.activate
    def test_context_manager(self) -> None:
        """APIClient should work as a context manager and close session on exit."""
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/ping",
            json={"ok": True},
            status=200,
        )
        with APIClient(base_url=BASE_URL, bearer_token="tok") as c:
            resp = c.get("/ping")
        assert resp.status_code == 200
