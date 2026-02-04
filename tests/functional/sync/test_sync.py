"""Functional tests for dvt sync: adapters, pyspark, and JDBC driver download."""
from pathlib import Path
from unittest import mock

import pytest

from dvt.tests.util import run_dbt


@pytest.fixture(scope="class")
def sync_venv(project_root):
    """Create a minimal .venv in project_root so sync finds an in-project env."""
    venv = Path(project_root) / ".venv"
    venv.mkdir(exist_ok=True)
    (venv / "bin").mkdir(exist_ok=True)
    py = venv / "bin" / "python"
    py.write_text("#!/usr/bin/env python3\n")
    try:
        py.chmod(0o755)
    except OSError:
        pass
    return venv


class TestSyncJdbcDrivers:
    """Sync downloads JDBC drivers for profile adapter types.

    Uses the standard project fixture (profile 'test', type 'postgres'). A .venv
    is created in the project so sync does not prompt for env path. Pip and pyspark
    are mocked so only the JDBC step runs for real (or is asserted via mock).
    """

    @pytest.mark.usefixtures("sync_venv")
    @mock.patch("dvt.task.sync.download_jdbc_jars")
    @mock.patch("dvt.task.sync._get_active_pyspark_version", return_value=None)
    @mock.patch("dvt.task.sync._run_uv_pip", return_value=True)
    @mock.patch("dvt.task.sync._run_pip", return_value=True)
    def test_sync_downloads_jdbc_jars_for_profile_adapters(
        self,
        mock_run_pip,
        mock_run_uv_pip,
        mock_pyspark_version,
        mock_download_jdbc_jars,
        project,
    ):
        """With postgres in profile, sync calls download_jdbc_jars with postgres driver."""
        run_dbt(["sync"])

        mock_download_jdbc_jars.assert_called_once()
        call = mock_download_jdbc_jars.call_args
        drivers = call[0][0]
        dest_dir = Path(call[0][1])
        assert len(drivers) >= 1
        assert any(c[0] == "org.postgresql" and c[1] == "postgresql" for c in drivers)
        # JDBC jars always go to canonical ~/.dvt/.jdbc_jars
        from dvt.config.user_config import get_jdbc_drivers_dir
        assert dest_dir == get_jdbc_drivers_dir(None)
        assert dest_dir.name == ".jdbc_jars"


class TestSyncNoVenvFailsOrPrompts:
    """Sync does not succeed when there is no in-project venv and no --python-env.

    Sync only looks for .venv/venv/env inside the project directory. If none is
    found and --python-env is not set, sync skips (non-interactive) or prompts.
    """

    def test_sync_fails_when_no_in_project_venv_and_no_python_env_flag(self, project):
        """With no .venv in project and no --python-env, sync does not succeed (non-interactive)."""
        # project has project_root; we do NOT use sync_venv, so there is no .venv in project_root.
        run_dbt(["sync"], expect_pass=False)

    def test_sync_succeeds_with_python_env_flag_pointing_to_valid_venv(
        self, project, tmp_path
    ):
        """With --python-env pointing to a valid venv, sync runs even when project has no .venv."""
        # Create a venv outside the project.
        project_root = Path(project.project_root)
        external_venv = tmp_path / "external_venv"
        external_venv.mkdir()
        (external_venv / "bin").mkdir()
        (external_venv / "bin" / "python").write_text("#!/usr/bin/env python3\n")
        (external_venv / "bin" / "python").chmod(0o755)

        with mock.patch("dvt.task.sync.download_jdbc_jars"):
            with mock.patch("dvt.task.sync._get_active_pyspark_version", return_value=None):
                with mock.patch("dvt.task.sync._run_uv_pip", return_value=True):
                    with mock.patch("dvt.task.sync._run_pip", return_value=True):
                        run_dbt(
                            ["sync", "--python-env", str(external_venv)],
                            expect_pass=True,
                        )
