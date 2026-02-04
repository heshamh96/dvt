"""Unit tests for adapter–JDBC driver registry and download (core/dvt/task/jdbc_drivers)."""
from pathlib import Path
from unittest import mock

import pytest

from dvt.task.jdbc_drivers import (
    ADAPTER_TO_JDBC_DRIVERS,
    download_jdbc_jars,
    get_jdbc_drivers_for_adapters,
)


class TestGetJdbcDriversForAdapters:
    """Tests for get_jdbc_drivers_for_adapters."""

    def test_empty_adapter_list_returns_empty(self):
        assert get_jdbc_drivers_for_adapters([]) == []

    def test_unknown_adapter_returns_empty(self):
        assert get_jdbc_drivers_for_adapters(["unknown"]) == []
        assert get_jdbc_drivers_for_adapters(["fake_adapter"]) == []

    def test_single_adapter_returns_related_driver(self):
        out = get_jdbc_drivers_for_adapters(["postgres"])
        assert len(out) == 1
        assert out[0] == ("org.postgresql", "postgresql", "42.7.3")

    def test_multiple_adapters_deduplicated(self):
        # postgres, greenplum, materialize all use org.postgresql:postgresql:42.7.3
        out = get_jdbc_drivers_for_adapters(["postgres", "greenplum", "materialize"])
        assert len(out) == 1
        assert out[0] == ("org.postgresql", "postgresql", "42.7.3")

    def test_multiple_adapters_different_drivers(self):
        out = get_jdbc_drivers_for_adapters(["postgres", "snowflake"])
        assert len(out) == 2
        coords = [tuple(c) for c in out]
        assert ("org.postgresql", "postgresql", "42.7.3") in coords
        assert ("net.snowflake", "snowflake-jdbc", "3.10.3") in coords

    def test_adapter_type_case_insensitive(self):
        out = get_jdbc_drivers_for_adapters(["Postgres", "SNOWFLAKE"])
        assert len(out) == 2

    def test_spark_not_in_registry_returns_nothing_for_spark(self):
        out = get_jdbc_drivers_for_adapters(["spark"])
        assert out == []

    def test_registry_has_expected_adapters(self):
        # Sanity: all entries in registry are non-empty and have valid tuple format
        for adapter, drivers in ADAPTER_TO_JDBC_DRIVERS.items():
            assert isinstance(drivers, list), f"{adapter} value should be a list"
            assert len(drivers) >= 1, f"{adapter} should have at least one driver"
            for coord in drivers:
                assert len(coord) == 3, f"{adapter} driver should be (group, artifact, version)"
                assert all(isinstance(c, str) for c in coord), f"{adapter} coords should be strings"


class TestDownloadJdbcJars:
    """Tests for download_jdbc_jars (with mocked network)."""

    def test_download_creates_jar_in_dest_dir(self, tmp_path):
        events = []

        with mock.patch("dvt.task.jdbc_drivers.urllib.request.urlopen") as urlopen:
            resp = mock.Mock()
            resp.status = 200
            resp.read.return_value = b"fake jar content"
            cm = mock.Mock()
            cm.__enter__.return_value = resp
            cm.__exit__.return_value = False
            urlopen.return_value = cm

            count = download_jdbc_jars(
                [("org.postgresql", "postgresql", "42.7.3")],
                tmp_path,
                on_event=events.append,
            )

        assert count == 1
        jar_path = tmp_path / "postgresql-42.7.3.jar"
        assert jar_path.exists()
        assert jar_path.read_bytes() == b"fake jar content"
        assert any("downloaded" in e for e in events)

    def test_skip_when_jar_already_exists(self, tmp_path):
        jar_path = tmp_path / "postgresql-42.7.3.jar"
        jar_path.write_bytes(b"existing")

        with mock.patch("dvt.task.jdbc_drivers.urllib.request.urlopen") as urlopen:
            count = download_jdbc_jars(
                [("org.postgresql", "postgresql", "42.7.3")],
                tmp_path,
                on_event=lambda _: None,
            )

        assert count == 1
        assert jar_path.read_bytes() == b"existing"
        urlopen.assert_not_called()

    def test_http_error_returns_zero_for_that_jar(self, tmp_path):
        import urllib.error

        events = []

        with mock.patch("dvt.task.jdbc_drivers.urllib.request.urlopen") as urlopen:
            urlopen.side_effect = urllib.error.HTTPError(
                None, 404, "Not Found", None, None
            )

            count = download_jdbc_jars(
                [("org.postgresql", "postgresql", "42.7.3")],
                tmp_path,
                on_event=events.append,
            )

        assert count == 0
        assert not (tmp_path / "postgresql-42.7.3.jar").exists()
        assert any("failed" in e for e in events)

    def test_multiple_drivers_download_both(self, tmp_path):
        events = []

        with mock.patch("dvt.task.jdbc_drivers.urllib.request.urlopen") as urlopen:
            resp = mock.Mock()
            resp.status = 200
            resp.read.return_value = b"jar"
            cm = mock.Mock()
            cm.__enter__.return_value = resp
            cm.__exit__.return_value = False
            urlopen.return_value = cm

            count = download_jdbc_jars(
                [
                    ("org.postgresql", "postgresql", "42.7.3"),
                    ("net.snowflake", "snowflake-jdbc", "3.10.3"),
                ],
                tmp_path,
                on_event=events.append,
            )

        assert count == 2
        assert (tmp_path / "postgresql-42.7.3.jar").exists()
        assert (tmp_path / "snowflake-jdbc-3.10.3.jar").exists()
