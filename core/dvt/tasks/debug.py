"""
DVT Debug Task — check connections to all targets or a specific target.

dvt debug              → tests ALL connections in profiles.yml
dvt debug --target X   → tests only connection X
"""

import time
from typing import Any, Dict, List, Optional, Tuple

from dvt.extraction.connection_mapper import map_to_sling_url, supported_types
from dvt.sync.profiles_reader import (
    default_profiles_dir,
    extract_outputs,
    read_profiles_yml,
)
from dvt.sync.sling_checker import check_sling


class DvtDebugTask:
    """Check connections to all targets in profiles.yml."""

    def __init__(self, flags: Any) -> None:
        self.flags = flags
        self.profiles_dir = (
            getattr(flags, "PROFILES_DIR", None) or default_profiles_dir()
        )
        self.target_filter = getattr(flags, "TARGET", None)

    def run(self) -> Any:
        print(f"\ndvt debug — checking connections from: {self.profiles_dir}\n")

        # Read profiles.yml
        try:
            raw_profiles = read_profiles_yml(self.profiles_dir)
        except FileNotFoundError as e:
            print(f"  ERROR: {e}")
            return {"success": False}
        except Exception as e:
            print(f"  ERROR reading profiles.yml: {e}")
            return {"success": False}

        outputs = extract_outputs(raw_profiles)
        if not outputs:
            print("  No outputs found in profiles.yml.")
            return {"success": True}

        # Filter to specific target if --target is set
        if self.target_filter:
            outputs = [o for o in outputs if o.get("_name") == self.target_filter]
            if not outputs:
                print(f"  Target '{self.target_filter}' not found in profiles.yml.")
                return {"success": False}

        # Check Sling availability
        sling_ok, sling_version = check_sling()
        print(f"  Sling: {'sling ' + sling_version if sling_ok else 'NOT FOUND'}")

        # Check DuckDB
        try:
            import duckdb

            print(f"  DuckDB: {duckdb.__version__}")
        except ImportError:
            print("  DuckDB: NOT FOUND")
        print()

        # Test each connection
        all_ok = True
        results: List[Tuple[str, str, str, str]] = []  # (name, type, status, detail)

        print("  Connections:")
        for output in outputs:
            name = output.get("_name", "?")
            adapter_type = output.get("type", "?")
            profile = output.get("_profile", "?")

            status, detail, elapsed = self._test_connection(output)
            results.append((name, adapter_type, status, detail))

            icon = "[OK]" if status == "ok" else "[FAILED]"
            pad = "." * max(1, 30 - len(name))
            elapsed_str = f"{elapsed:.1f}s" if elapsed else ""

            if status == "ok":
                print(f"    {name} {pad} {icon} ({adapter_type}, {elapsed_str})")
            elif status == "skip":
                print(f"    {name} {pad} [SKIP] ({detail})")
            else:
                print(f"    {name} {pad} {icon} ({adapter_type})")
                print(f"      Error: {detail}")
                all_ok = False

        print()
        ok_count = sum(1 for _, _, s, _ in results if s == "ok")
        fail_count = sum(1 for _, _, s, _ in results if s == "failed")
        skip_count = sum(1 for _, _, s, _ in results if s == "skip")
        total = len(results)

        if all_ok:
            print(f"  All {ok_count} connection(s) OK.")
        else:
            print(
                f"  {ok_count} OK, {fail_count} FAILED, {skip_count} SKIPPED out of {total}."
            )

        return {"success": all_ok, "results": results}

    def _test_connection(self, output: Dict[str, Any]) -> Tuple[str, str, float]:
        """Test a single connection. Returns (status, detail, elapsed_seconds).

        status: 'ok', 'failed', 'skip'
        """
        adapter_type = output.get("type", "")
        name = output.get("_name", "?")

        # Bucket types — test differently
        if adapter_type in ("s3", "gcs", "azure"):
            return self._test_bucket(output)

        # Database types — test via Sling or DuckDB ATTACH
        if adapter_type not in supported_types():
            return "skip", f"unsupported type '{adapter_type}'", 0.0

        # Try Sling connection test
        try:
            url = map_to_sling_url(output)
        except Exception as e:
            return "failed", f"cannot build connection URL: {e}", 0.0

        return self._test_db_via_sling(name, url, adapter_type)

    def _test_db_via_sling(
        self, name: str, url: str, adapter_type: str
    ) -> Tuple[str, str, float]:
        """Test a database connection via Sling."""
        sling_ok, _ = check_sling()
        if not sling_ok:
            # Fallback: try DuckDB ATTACH for supported types
            if adapter_type in ("postgres", "mysql", "sqlite"):
                return self._test_db_via_duckdb(name, url, adapter_type)
            return "skip", "Sling not available", 0.0

        try:
            from sling import SLING_BIN
            import subprocess

            start = time.time()
            # Use sling conns test with the URL
            result = subprocess.run(
                [SLING_BIN, "conns", "exec", name, "select 1"],
                capture_output=True,
                text=True,
                timeout=15,
                env={
                    **__import__("os").environ,
                    f"SLING_CONNECTION_{name.upper()}": url,
                    # Also set as the connection name directly
                    name: url,
                },
            )
            elapsed = time.time() - start

            if result.returncode == 0:
                return "ok", "", elapsed
            else:
                error = result.stderr.strip() or result.stdout.strip()
                # Try a simpler approach — just construct the URL and let Sling test it
                return self._test_db_via_sling_replication(name, url, adapter_type)

        except Exception as e:
            return self._test_db_via_sling_replication(name, url, adapter_type)

    def _test_db_via_sling_replication(
        self, name: str, url: str, adapter_type: str
    ) -> Tuple[str, str, float]:
        """Test a database connection via Sling Replication with a SELECT 1 query."""
        try:
            from sling import Replication, ReplicationStream
            import tempfile
            import os

            start = time.time()
            # Create a minimal replication that just reads SELECT 1 from the source
            # and writes to a temp file
            tmp = tempfile.mktemp(suffix=".csv")
            try:
                # Use dialect-appropriate ping query
                if "oracle" in adapter_type:
                    ping_sql = "SELECT 1 FROM DUAL"
                else:
                    ping_sql = "SELECT 1"

                replication = Replication(
                    source=url,
                    target=f"file://.",
                    streams={
                        "custom_sql": ReplicationStream(
                            sql=ping_sql,
                            object=tmp,
                            mode="full-refresh",
                        ),
                    },
                )
                replication.run()
                elapsed = time.time() - start
                return "ok", "", elapsed
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

        except Exception as e:
            elapsed = time.time() - start if "start" in dir() else 0.0
            error_msg = str(e)
            # Extract just the useful part of the error
            if "could not connect" in error_msg.lower():
                return "failed", "could not connect to database", elapsed
            return "failed", error_msg[:200], elapsed

    def _test_db_via_duckdb(
        self, name: str, url: str, adapter_type: str
    ) -> Tuple[str, str, float]:
        """Test a database connection via DuckDB ATTACH (fallback for pg/mysql/sqlite)."""
        try:
            import duckdb

            start = time.time()
            conn = duckdb.connect(":memory:")
            # DuckDB ATTACH doesn't use Sling URLs — need native format
            # This is a basic fallback, not the primary path
            conn.execute("SELECT 1")
            elapsed = time.time() - start
            conn.close()
            return (
                "skip",
                "use dvt sync to install Sling for full connection test",
                elapsed,
            )
        except Exception as e:
            return "skip", f"DuckDB fallback failed: {e}", 0.0

    def _test_bucket(self, output: Dict[str, Any]) -> Tuple[str, str, float]:
        """Test a bucket connection (S3, GCS, Azure)."""
        adapter_type = output.get("type", "")

        if adapter_type == "s3":
            try:
                import boto3

                start = time.time()
                bucket = output.get("bucket", "")
                region = output.get("region", "us-east-1")
                s3 = boto3.client(
                    "s3",
                    region_name=region,
                    aws_access_key_id=output.get("access_key_id"),
                    aws_secret_access_key=output.get("secret_access_key"),
                )
                s3.head_bucket(Bucket=bucket)
                elapsed = time.time() - start
                return "ok", "", elapsed
            except ImportError:
                return "skip", "boto3 not installed", 0.0
            except Exception as e:
                return "failed", str(e)[:200], 0.0

        elif adapter_type == "gcs":
            try:
                from google.cloud import storage

                start = time.time()
                client = storage.Client(project=output.get("project"))
                bucket = client.get_bucket(output.get("bucket", ""))
                elapsed = time.time() - start
                return "ok", "", elapsed
            except ImportError:
                return "skip", "google-cloud-storage not installed", 0.0
            except Exception as e:
                return "failed", str(e)[:200], 0.0

        return "skip", f"bucket type '{adapter_type}' not testable", 0.0

    @staticmethod
    def interpret_results(results: Any) -> bool:
        if results is None:
            return False
        if isinstance(results, dict):
            return results.get("success", False)
        return True
