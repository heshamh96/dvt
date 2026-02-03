import pytest

from dvt.exceptions import CompilationError
from dvt.tests.util import run_dbt

exposure_dupes_schema_yml = """
version: 2
exposures:
  - name: something
    type: dashboard
    owner:
      email: test@example.com
  - name: something
    type: dashboard
    owner:
      email: test@example.com

"""


class TestDuplicateExposure:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": exposure_dupes_schema_yml}

    def test_duplicate_exposure(self, project):
        message = "dvt found two exposures with the name"
        with pytest.raises(CompilationError) as exc:
            run_dbt(["compile"])
        assert message in str(exc.value)
