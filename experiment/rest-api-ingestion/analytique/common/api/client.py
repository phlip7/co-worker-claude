"""Generic REST API client for data ingestion.

Provides reusable functionality for fetching data from REST APIs with support for
pagination, retry logic, and authentication. Can be used across all ingestion projects.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Generator, Optional, Protocol

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from analytique.common.utils.logger import get_logger


class APIClientError(Exception):
    """Custom exception for API client errors."""

    pass


@dataclass
class APIClientConfig:
    """Generic API client configuration."""

    base_url: str
    auth_type: str = "bearer"
    auth_token: Optional[str] = None
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    page_size: int = 1000


class PaginationConfig(Protocol):
    """Protocol for pagination configuration."""

    endpoint: str
    pagination_type: str
    pagination_param: str
    page_size_param: str
    change_tracking_column: Optional[str]


class APIClient:
    """Generic REST API client with pagination and retry support."""

    def __init__(self, config: APIClientConfig):
        """Initialize the API client.

        Args:
            config: API client configuration.
        """
        self.config = config
        self.logger = get_logger()
        self._session: Optional[requests.Session] = None

    @property
    def session(self) -> requests.Session:
        """Get or create the requests session with retry configuration.

        Returns:
            Configured requests session.
        """
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry and timeout configuration.

        Returns:
            Configured requests session.
        """
        session = requests.Session()

        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        if self.config.auth_token:
            if self.config.auth_type.lower() == "bearer":
                session.headers.update({"Authorization": f"Bearer {self.config.auth_token}"})
            elif self.config.auth_type.lower() == "api_key":
                session.headers.update({"X-API-Key": self.config.auth_token})

        session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        return session

    def _build_url(self, endpoint: str) -> str:
        """Build the full URL for an API endpoint.

        Args:
            endpoint: API endpoint path.

        Returns:
            Full URL.
        """
        base_url = self.config.base_url.rstrip("/")
        endpoint = endpoint.lstrip("/")
        return f"{base_url}/{endpoint}"

    def fetch(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Fetch data from a single API endpoint.

        Args:
            endpoint: API endpoint path.
            params: Query parameters.

        Returns:
            JSON response data.

        Raises:
            APIClientError: If the request fails after retries.
        """
        url = self._build_url(endpoint)
        self.logger.debug(f"Fetching data from {url} with params {params}")

        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout as e:
            self.logger.error(f"Request timeout for {url}: {e}")
            raise APIClientError(f"Request timeout: {e}") from e

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error for {url}: {e}")
            raise APIClientError(f"HTTP error: {e}") from e

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            raise APIClientError(f"Request failed: {e}") from e

    def fetch_with_pagination(
        self,
        pagination_config: PaginationConfig,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        watermark_value: Optional[Any] = None,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Fetch data from an API endpoint with pagination support.

        Yields batches of records as they are fetched from each page.

        Args:
            pagination_config: Configuration for pagination (endpoint, type, params).
            start_date: Start date for filtering (optional).
            end_date: End date for filtering (optional).
            watermark_value: Last known value for incremental loads (optional).

        Yields:
            List of records for each page.

        Raises:
            APIClientError: If fetching fails.
        """
        endpoint = pagination_config.endpoint
        page_size = self.config.page_size
        pagination_type = pagination_config.pagination_type

        if pagination_type == "offset":
            yield from self._fetch_with_offset_pagination(
                endpoint=endpoint,
                pagination_config=pagination_config,
                page_size=page_size,
                start_date=start_date,
                end_date=end_date,
                watermark_value=watermark_value,
            )
        elif pagination_type == "cursor":
            yield from self._fetch_with_cursor_pagination(
                endpoint=endpoint,
                pagination_config=pagination_config,
                page_size=page_size,
                start_date=start_date,
                end_date=end_date,
                watermark_value=watermark_value,
            )
        elif pagination_type == "page":
            yield from self._fetch_with_page_pagination(
                endpoint=endpoint,
                pagination_config=pagination_config,
                page_size=page_size,
                start_date=start_date,
                end_date=end_date,
                watermark_value=watermark_value,
            )
        else:
            raise APIClientError(f"Unsupported pagination type: {pagination_type}")

    def _build_date_params(
        self,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        watermark_value: Optional[Any],
        change_tracking_column: Optional[str],
    ) -> dict[str, Any]:
        """Build query parameters for date filtering.

        Args:
            start_date: Start date for filtering.
            end_date: End date for filtering.
            watermark_value: Last known value for incremental loads.
            change_tracking_column: Column name used for change tracking.

        Returns:
            Dictionary of query parameters.
        """
        params: dict[str, Any] = {}

        if start_date:
            params["start_date"] = start_date.strftime("%Y-%m-%d")
        if end_date:
            params["end_date"] = end_date.strftime("%Y-%m-%d")

        if watermark_value and change_tracking_column:
            if isinstance(watermark_value, datetime):
                params[f"{change_tracking_column}_after"] = watermark_value.isoformat()
            else:
                params[f"{change_tracking_column}_after"] = str(watermark_value)

        return params

    def _fetch_with_offset_pagination(
        self,
        endpoint: str,
        pagination_config: PaginationConfig,
        page_size: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        watermark_value: Optional[Any] = None,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Fetch data using offset-based pagination."""
        offset = 0
        page_num = 0

        while True:
            params = self._build_date_params(
                start_date, end_date, watermark_value, pagination_config.change_tracking_column
            )
            params[pagination_config.pagination_param] = offset
            params[pagination_config.page_size_param] = page_size

            self.logger.info(f"Fetching page {page_num + 1} (offset={offset})")
            response = self.fetch(endpoint, params)
            records = self._extract_records(response)

            if not records:
                self.logger.info("No more records to fetch")
                break

            yield records

            if len(records) < page_size:
                self.logger.info("Reached last page (partial page returned)")
                break

            offset += page_size
            page_num += 1

    def _fetch_with_cursor_pagination(
        self,
        endpoint: str,
        pagination_config: PaginationConfig,
        page_size: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        watermark_value: Optional[Any] = None,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Fetch data using cursor-based pagination."""
        cursor: Optional[str] = None
        page_num = 0

        while True:
            params = self._build_date_params(
                start_date, end_date, watermark_value, pagination_config.change_tracking_column
            )
            params[pagination_config.page_size_param] = page_size

            if cursor:
                params["cursor"] = cursor

            self.logger.info(f"Fetching page {page_num + 1} (cursor={cursor})")
            response = self.fetch(endpoint, params)
            records = self._extract_records(response)

            if not records:
                self.logger.info("No more records to fetch")
                break

            yield records

            cursor = response.get("next_cursor") or response.get("cursor")
            if not cursor:
                self.logger.info("No next cursor, reached end of data")
                break

            page_num += 1

    def _fetch_with_page_pagination(
        self,
        endpoint: str,
        pagination_config: PaginationConfig,
        page_size: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        watermark_value: Optional[Any] = None,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Fetch data using page number-based pagination."""
        page_num = 1

        while True:
            params = self._build_date_params(
                start_date, end_date, watermark_value, pagination_config.change_tracking_column
            )
            params["page"] = page_num
            params[pagination_config.page_size_param] = page_size

            self.logger.info(f"Fetching page {page_num}")
            response = self.fetch(endpoint, params)
            records = self._extract_records(response)

            if not records:
                self.logger.info("No more records to fetch")
                break

            yield records

            total_pages = response.get("total_pages") or response.get("pages")
            if total_pages and page_num >= total_pages:
                self.logger.info("Reached last page")
                break

            if len(records) < page_size:
                self.logger.info("Reached last page (partial page returned)")
                break

            page_num += 1

    def _extract_records(self, response: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract records from API response.

        Handles various response formats (direct list, nested in 'data', 'results', etc.)

        Args:
            response: API response dictionary.

        Returns:
            List of record dictionaries.
        """
        if isinstance(response, list):
            return response

        for key in ["data", "results", "items", "records"]:
            if key in response and isinstance(response[key], list):
                return response[key]

        return []

    def close(self) -> None:
        """Close the API client session."""
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self) -> "APIClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
