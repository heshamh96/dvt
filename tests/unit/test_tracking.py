import datetime
import tempfile

import pytest

import dvt.tracking


@pytest.fixture(scope="function")
def active_user_none() -> None:
    dvt.tracking.active_user = None


@pytest.fixture(scope="function")
def tempdir(active_user_none) -> str:
    return tempfile.mkdtemp()


class TestTracking:
    def test_tracking_initial(self, tempdir):
        assert dvt.tracking.active_user is None
        dvt.tracking.initialize_from_flags(True, tempdir)
        assert isinstance(dvt.tracking.active_user, dvt.tracking.User)

        invocation_id = dvt.tracking.active_user.invocation_id
        run_started_at = dvt.tracking.active_user.run_started_at

        assert dvt.tracking.active_user.do_not_track is False
        assert isinstance(dvt.tracking.active_user.id, str)
        assert isinstance(invocation_id, str)
        assert isinstance(run_started_at, datetime.datetime)

        dvt.tracking.disable_tracking()
        assert isinstance(dvt.tracking.active_user, dvt.tracking.User)

        assert dvt.tracking.active_user.do_not_track is True
        assert dvt.tracking.active_user.id is None
        assert dvt.tracking.active_user.invocation_id == invocation_id
        assert dvt.tracking.active_user.run_started_at == run_started_at

        # this should generate a whole new user object -> new run_started_at
        dvt.tracking.do_not_track()
        assert isinstance(dvt.tracking.active_user, dvt.tracking.User)

        assert dvt.tracking.active_user.do_not_track is True
        assert dvt.tracking.active_user.id is None
        assert isinstance(dvt.tracking.active_user.invocation_id, str)
        assert isinstance(dvt.tracking.active_user.run_started_at, datetime.datetime)
        # invocation_id no longer only linked to active_user so it doesn't change
        assert dvt.tracking.active_user.invocation_id == invocation_id
        # if you use `!=`, you might hit a race condition (especially on windows)
        assert dvt.tracking.active_user.run_started_at is not run_started_at

    def test_tracking_never_ok(self, active_user_none):
        assert dvt.tracking.active_user is None

        # this should generate a whole new user object -> new invocation_id/run_started_at
        dvt.tracking.do_not_track()
        assert isinstance(dvt.tracking.active_user, dvt.tracking.User)

        assert dvt.tracking.active_user.do_not_track is True
        assert dvt.tracking.active_user.id is None
        assert isinstance(dvt.tracking.active_user.invocation_id, str)
        assert isinstance(dvt.tracking.active_user.run_started_at, datetime.datetime)

    def test_disable_never_enabled(self, active_user_none):
        assert dvt.tracking.active_user is None

        # this should generate a whole new user object -> new invocation_id/run_started_at
        dvt.tracking.disable_tracking()
        assert isinstance(dvt.tracking.active_user, dvt.tracking.User)

        assert dvt.tracking.active_user.do_not_track is True
        assert dvt.tracking.active_user.id is None
        assert isinstance(dvt.tracking.active_user.invocation_id, str)
        assert isinstance(dvt.tracking.active_user.run_started_at, datetime.datetime)

    @pytest.mark.parametrize("send_anonymous_usage_stats", [True, False])
    def test_initialize_from_flags(self, tempdir, send_anonymous_usage_stats):
        dvt.tracking.initialize_from_flags(send_anonymous_usage_stats, tempdir)
        assert dvt.tracking.active_user.do_not_track != send_anonymous_usage_stats
