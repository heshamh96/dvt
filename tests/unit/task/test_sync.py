"""Unit tests for dvt sync task and JDBC integration (core/dvt/task/sync)."""
from pathlib import Path
from unittest import mock

import pytest

from dvt.cli.flags import Flags
from dvt.task.sync import SyncTask


@pytest.fixture
def mock_flags(tmp_path):
    """Flags with project_dir and profiles_dir set to temp paths."""
    flags = mock.Mock(spec=Flags)
    flags.project_dir = str(tmp_path / "project")
    flags.PROFILES_DIR = str(tmp_path / "profiles")
    flags.PROJECT_DIR = str(tmp_path / "project")
    return flags


class TestSyncTaskJdbcIntegration:
    """Sync task calls JDBC driver resolution and download for profile adapters."""

    @mock.patch("dvt.task.sync.download_jdbc_jars")
    @mock.patch("dvt.task.sync.get_jdbc_drivers_for_adapters")
    @mock.patch("dvt.task.sync._get_active_pyspark_version", return_value=None)
    @mock.patch("dvt.task.sync._run_uv_pip", return_value=True)
    @mock.patch("dvt.task.sync._run_pip", return_value=True)
    @mock.patch("dvt.task.sync._detect_package_manager", return_value="pip")
    @mock.patch("dvt.task.sync._get_require_adapters", return_value={})
    @mock.patch("dvt.task.sync._get_adapter_types_from_profile")
    @mock.patch("dvt.task.sync._get_profile_name")
    @mock.patch("dvt.task.sync._get_env_python")
    @mock.patch("dvt.task.sync._find_project_env")
    @mock.patch("dvt.task.sync.get_nearest_project_dir")
    def test_sync_calls_jdbc_download_for_profile_adapters(
        self,
        mock_get_project_dir,
        mock_find_env,
        mock_get_env_python,
        mock_get_profile_name,
        mock_get_adapter_types,
        mock_detect_pm,
        mock_run_pip,
        mock_run_uv_pip,
        mock_get_require_adapters,
        mock_get_pyspark_version,
        mock_get_jdbc_drivers,
        mock_download_jdbc_jars,
        mock_flags,
        tmp_path,
    ):
        project_root = tmp_path / "project"
        project_root.mkdir()
        env_path = tmp_path / "venv"
        env_path.mkdir()
        (env_path / "bin").mkdir()
        (env_path / "bin" / "python").write_text("")
        (env_path / "bin" / "python").chmod(0o755)

        mock_get_project_dir.return_value = project_root
        mock_find_env.return_value = env_path
        mock_get_env_python.return_value = env_path / "bin" / "python"
        mock_get_profile_name.return_value = "my_profile"
        mock_get_adapter_types.return_value = ["postgres", "snowflake"]
        mock_get_jdbc_drivers.return_value = [
            ("org.postgresql", "postgresql", "42.7.3"),
            ("net.snowflake", "snowflake-jdbc", "3.10.3"),
        ]

        # Ensure profiles_dir exists for get_jdbc_drivers_dir
        profiles_dir = Path(mock_flags.PROFILES_DIR)
        profiles_dir.mkdir(parents=True, exist_ok=True)

        task = SyncTask(mock_flags)
        task.run()

        mock_get_jdbc_drivers.assert_called_once_with(["postgres", "snowflake"])
        mock_download_jdbc_jars.assert_called_once()
        call_args = mock_download_jdbc_jars.call_args
        assert call_args[0][0] == [
            ("org.postgresql", "postgresql", "42.7.3"),
            ("net.snowflake", "snowflake-jdbc", "3.10.3"),
        ]
        # JDBC jars always go to canonical DVT home ~/.dvt/.jdbc_jars
        from dvt.config.user_config import get_jdbc_drivers_dir
        assert call_args[0][1] == get_jdbc_drivers_dir(None)
        assert call_args[1]["on_event"] is not None

    @mock.patch("dvt.task.sync.download_jdbc_jars")
    @mock.patch("dvt.task.sync.get_jdbc_drivers_for_adapters", return_value=[])
    @mock.patch("dvt.task.sync._get_active_pyspark_version", return_value=None)
    @mock.patch("dvt.task.sync._run_uv_pip", return_value=True)
    @mock.patch("dvt.task.sync._run_pip", return_value=True)
    @mock.patch("dvt.task.sync._detect_package_manager", return_value="pip")
    @mock.patch("dvt.task.sync._get_require_adapters", return_value={})
    @mock.patch("dvt.task.sync._get_adapter_types_from_profile", return_value=["spark"])
    @mock.patch("dvt.task.sync._get_profile_name")
    @mock.patch("dvt.task.sync._get_env_python")
    @mock.patch("dvt.task.sync._find_project_env")
    @mock.patch("dvt.task.sync.get_nearest_project_dir")
    def test_sync_does_not_call_download_when_no_jdbc_drivers_for_adapters(
        self,
        mock_get_project_dir,
        mock_find_env,
        mock_get_env_python,
        mock_get_profile_name,
        mock_get_adapter_types,
        mock_get_require_adapters,
        mock_detect_pm,
        mock_run_pip,
        mock_run_uv_pip,
        mock_get_pyspark_version,
        mock_get_jdbc_drivers,
        mock_download_jdbc_jars,
        mock_flags,
        tmp_path,
    ):
        project_root = tmp_path / "project"
        project_root.mkdir()
        env_path = tmp_path / "venv"
        env_path.mkdir()
        (env_path / "bin").mkdir()
        (env_path / "bin" / "python").write_text("")
        (env_path / "bin" / "python").chmod(0o755)

        mock_get_project_dir.return_value = project_root
        mock_find_env.return_value = env_path
        mock_get_env_python.return_value = env_path / "bin" / "python"
        mock_get_profile_name.return_value = "my_profile"
        Path(mock_flags.PROFILES_DIR).mkdir(parents=True, exist_ok=True)

        task = SyncTask(mock_flags)
        task.run()

        mock_get_jdbc_drivers.assert_called_once_with(["spark"])
        mock_download_jdbc_jars.assert_not_called()

    @mock.patch("dvt.task.sync.download_jdbc_jars")
    @mock.patch("dvt.task.sync.get_jdbc_drivers_for_adapters", return_value=[])
    @mock.patch("dvt.task.sync._get_active_pyspark_version", return_value=None)
    @mock.patch("dvt.task.sync._run_uv_pip", return_value=True)
    @mock.patch("dvt.task.sync._run_pip", return_value=True)
    @mock.patch("dvt.task.sync._detect_package_manager", return_value="pip")
    @mock.patch("dvt.task.sync._get_require_adapters", return_value={})
    @mock.patch("dvt.task.sync._get_adapter_types_from_profile", return_value=[])
    @mock.patch("dvt.task.sync._get_profile_name")
    @mock.patch("dvt.task.sync._get_env_python")
    @mock.patch("dvt.task.sync._find_project_env")
    @mock.patch("dvt.task.sync.get_nearest_project_dir")
    def test_sync_uses_python_env_flag_and_skips_in_project_lookup(
        self,
        mock_get_project_dir,
        mock_find_env,
        mock_get_env_python,
        mock_get_profile_name,
        mock_get_adapter_types,
        mock_run_pip,
        mock_run_uv_pip,
        mock_get_require_adapters,
        mock_get_pyspark_version,
        mock_get_jdbc_drivers,
        mock_download_jdbc_jars,
        mock_flags,
        tmp_path,
    ):
        """When --python-env is set, sync uses it and does not look for .venv inside project."""
        project_root = tmp_path / "project"
        project_root.mkdir()
        explicit_venv = tmp_path / "external_venv"
        explicit_venv.mkdir()
        (explicit_venv / "bin").mkdir()
        (explicit_venv / "bin" / "python").write_text("")
        (explicit_venv / "bin" / "python").chmod(0o755)

        mock_get_project_dir.return_value = project_root
        mock_get_env_python.return_value = explicit_venv / "bin" / "python"
        mock_get_profile_name.return_value = "my_profile"
        Path(mock_flags.PROFILES_DIR).mkdir(parents=True, exist_ok=True)
        mock_flags.PYTHON_ENV = str(explicit_venv)

        task = SyncTask(mock_flags)
        task.run()

        mock_find_env.assert_not_called()
        assert task.env_path == explicit_venv
