import copy
import os
import re
import shutil
from pathlib import Path
from typing import Optional

import click
import yaml

import dvt.config
import dbt_common.clients.system
from dvt.adapters.factory import get_include_paths, load_plugin

# read_profile import removed - check_if_profile_exists now handles empty profiles.yml gracefully
from dvt.contracts.util import Identifier as ProjectName
from dvt.events.types import (
    ConfigFolderDirectory,
    InvalidProfileTemplateYAML,
    NoSampleProfileFound,
    ProfileWrittenWithProjectTemplateYAML,
    ProfileWrittenWithSample,
    ProfileWrittenWithTargetTemplateYAML,
    ProjectCreated,
    ProjectNameAlreadyExists,
    SettingUpProfile,
    StarterProjectPath,
)
from dvt.config.user_config import (
    append_profile_to_buckets_yml,
    append_profile_to_computes_yml,
    append_profile_to_profiles_yml,
    create_default_buckets_yml,
    create_default_computes_yml,
    create_default_profiles_yml,
    create_dvt_data_dir,
    init_mdm_db,
)
from dvt.constants import DBT_PROJECT_FILE_NAME, DVT_PROJECT_FILE_NAME
from dvt.config.project import get_project_yml_path
from dvt.flags import get_flags
from dvt.task.base import BaseTask
from dvt.version import _get_adapter_plugin_names
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtRuntimeError

DOCS_URL = "https://docs.getdbt.com/docs/configure-your-profile"
SLACK_URL = "https://community.getdbt.com/"

# This file is not needed for the starter project but exists for finding the resource path
IGNORE_FILES = ["__init__.py", "__pycache__"]


# https://click.palletsprojects.com/en/8.0.x/api/#types
# click v7.0 has UNPROCESSED, STRING, INT, FLOAT, BOOL, and UUID available.
click_type_mapping = {
    "string": click.STRING,
    "int": click.INT,
    "float": click.FLOAT,
    "bool": click.BOOL,
    None: None,
}


class InitTask(BaseTask):
    def copy_starter_repo(self, project_name: str) -> None:
        # Lazy import to avoid ModuleNotFoundError
        from dvt.include.starter_project import (
            PACKAGE_PATH as starter_project_directory,
        )

        fire_event(StarterProjectPath(dir=starter_project_directory))
        shutil.copytree(
            starter_project_directory,
            project_name,
            ignore=shutil.ignore_patterns(*IGNORE_FILES),
        )

    def create_profiles_dir(self, profiles_dir: str) -> bool:
        """Create the user's profiles directory if it doesn't already exist."""
        profiles_path = Path(profiles_dir)
        if not profiles_path.exists():
            fire_event(ConfigFolderDirectory(dir=str(profiles_dir)))
            dbt_common.clients.system.make_directory(profiles_dir)
            return True
        return False

    def create_dvt_user_config(self, profiles_dir: str) -> None:
        """Create DVT user-level config files in the profiles dir.

        Creates header-only templates for:
        - profiles.yml (database connections)
        - computes.yml (Spark compute engines)
        - buckets.yml (staging buckets)
        - data/ directory and mdm.duckdb
        """
        profiles_path = Path(str(profiles_dir))
        if not profiles_path.exists():
            return
        dvt_home = profiles_path.resolve()
        # Create profiles.yml header if it doesn't exist
        profiles_yml_path = dvt_home / "profiles.yml"
        if create_default_profiles_yml(profiles_yml_path):
            fire_event(ConfigFolderDirectory(dir=str(profiles_yml_path.parent)))
        # Create computes.yml header if it doesn't exist
        computes_path = dvt_home / "computes.yml"
        if create_default_computes_yml(computes_path):
            fire_event(ConfigFolderDirectory(dir=str(computes_path.parent)))
        # Create buckets.yml header if it doesn't exist
        buckets_path = dvt_home / "buckets.yml"
        if create_default_buckets_yml(buckets_path):
            fire_event(ConfigFolderDirectory(dir=str(buckets_path.parent)))
        # Create data directory and MDM database
        create_dvt_data_dir(str(dvt_home))
        init_mdm_db(str(dvt_home))

    def create_profile_from_sample(self, adapter: str, profile_name: str):
        """Create a profile entry using the adapter's sample_profiles.yml

        Renames the profile in sample_profiles.yml to match that of the project."""
        # Line below raises an exception if the specified adapter is not found
        load_plugin(adapter)
        adapter_path = get_include_paths(adapter)[0]
        sample_profiles_path = adapter_path / "sample_profiles.yml"

        if not sample_profiles_path.exists():
            fire_event(NoSampleProfileFound(adapter=adapter))
        else:
            with open(sample_profiles_path, "r") as f:
                sample_profile = f.read()
            sample_profile_name = list(yaml.safe_load(sample_profile).keys())[0]
            # Use a regex to replace the name of the sample_profile with
            # that of the project without losing any comments from the sample
            sample_profile = re.sub(
                f"^{sample_profile_name}:", f"{profile_name}:", sample_profile
            )
            profiles_filepath = Path(get_flags().PROFILES_DIR) / Path("profiles.yml")
            if profiles_filepath.exists():
                with open(profiles_filepath, "a") as f:
                    f.write("\n" + sample_profile)
            else:
                with open(profiles_filepath, "w") as f:
                    f.write(sample_profile)
                fire_event(
                    ProfileWrittenWithSample(
                        name=profile_name, path=str(profiles_filepath)
                    )
                )

    def generate_target_from_input(
        self, profile_template: dict, target: dict = {}
    ) -> dict:
        """Generate a target configuration from profile_template and user input."""
        profile_template_local = copy.deepcopy(profile_template)
        for key, value in profile_template_local.items():
            if key.startswith("_choose"):
                choice_type = key[8:].replace("_", " ")
                option_list = list(value.keys())
                prompt_msg = (
                    "\n".join([f"[{n + 1}] {v}" for n, v in enumerate(option_list)])
                    + f"\nDesired {choice_type} option (enter a number)"
                )
                numeric_choice = click.prompt(prompt_msg, type=click.INT)
                choice = option_list[numeric_choice - 1]
                # Complete the chosen option's values in a recursive call
                target = self.generate_target_from_input(
                    profile_template_local[key][choice], target
                )
            else:
                if key.startswith("_fixed"):
                    # _fixed prefixed keys are not presented to the user
                    target[key[7:]] = value
                else:
                    hide_input = value.get("hide_input", False)
                    default = value.get("default", None)
                    hint = value.get("hint", None)
                    type = click_type_mapping[value.get("type", None)]
                    text = key + (f" ({hint})" if hint else "")
                    target[key] = click.prompt(
                        text, default=default, hide_input=hide_input, type=type
                    )
        return target

    def get_profile_name_from_current_project(self) -> str:
        """Reads dbt_project.yml (or dvt_project.yml) in the current directory
        to retrieve the profile name.
        """
        project_yml_path = get_project_yml_path(os.getcwd())
        with open(project_yml_path) as f:
            dbt_project = yaml.safe_load(f)
        return dbt_project["profile"]

    def write_profile(self, profile: dict, profile_name: str) -> bool:
        """Append a profile to profiles.yml if it doesn't already exist.

        Skips silently if the profile already exists (consistent with computes/buckets).

        Returns True if profile was written, False if skipped.
        """
        profiles_filepath = Path(get_flags().PROFILES_DIR) / Path("profiles.yml")

        profiles = {}
        if profiles_filepath.exists():
            with open(profiles_filepath, "r") as f:
                profiles = yaml.safe_load(f) or {}

        # Skip if profile already exists
        if profile_name in profiles:
            return False

        # Add the new profile
        profiles[profile_name] = profile

        # Write all profiles back to file
        with open(profiles_filepath, "w") as f:
            yaml.dump(profiles, f)

        return True

    def create_profile_from_profile_template(
        self, profile_template: dict, profile_name: str
    ):
        """Create and write a profile using the supplied profile_template."""
        initial_target = profile_template.get("fixed", {})
        prompts = profile_template.get("prompts", {})
        target = self.generate_target_from_input(prompts, initial_target)
        target_name = target.pop("target", "dev")
        profile = {"outputs": {target_name: target}, "target": target_name}
        self.write_profile(profile, profile_name)

    def create_profile_from_target(self, adapter: str, profile_name: str):
        """Create a profile without defaults using target's profile_template.yml if available, or
        sample_profiles.yml as a fallback."""
        # Line below raises an exception if the specified adapter is not found
        load_plugin(adapter)
        adapter_path = get_include_paths(adapter)[0]
        profile_template_path = adapter_path / "profile_template.yml"

        if profile_template_path.exists():
            with open(profile_template_path) as f:
                profile_template = yaml.safe_load(f)
            self.create_profile_from_profile_template(profile_template, profile_name)
            profiles_filepath = Path(get_flags().PROFILES_DIR) / Path("profiles.yml")
            fire_event(
                ProfileWrittenWithTargetTemplateYAML(
                    name=profile_name, path=str(profiles_filepath)
                )
            )
        else:
            # For adapters without a profile_template.yml defined, fallback on
            # sample_profiles.yml
            self.create_profile_from_sample(adapter, profile_name)

    def check_if_profile_exists(self, profile_name: str) -> bool:
        """
        Check if the specified profile exists in profiles.yml.

        Returns False if:
        - profiles.yml doesn't exist
        - profiles.yml is empty or has only comments
        - profile_name is not in profiles.yml

        Returns True only if profile_name exists as an actual (uncommented) profile.
        """
        profiles_file = Path(get_flags().PROFILES_DIR) / "profiles.yml"
        if not profiles_file.exists():
            return False
        try:
            with open(profiles_file, "r") as f:
                data = yaml.safe_load(f)
            # yaml.safe_load returns None for empty/comment-only files
            if not data or not isinstance(data, dict):
                return False
            return profile_name in data
        except Exception:
            return False

    def check_if_can_write_profile(self, profile_name: Optional[str] = None) -> bool:
        """Using either a provided profile name or that specified in dbt_project.yml,
        check if the profile already exists in profiles.yml, and if so ask the
        user whether to proceed and overwrite it."""
        profiles_file = Path(get_flags().PROFILES_DIR) / Path("profiles.yml")
        if not profiles_file.exists():
            return True
        profile_name = profile_name or self.get_profile_name_from_current_project()
        with open(profiles_file, "r") as f:
            profiles = yaml.safe_load(f) or {}
        if profile_name in profiles.keys():
            response = click.confirm(
                f"The profile {profile_name} already exists in "
                f"{profiles_file}. Continue and overwrite it?"
            )
            return response
        else:
            return True

    def create_profile_using_project_profile_template(self, profile_name):
        """Create a profile using the project's profile_template.yml"""
        with open("profile_template.yml") as f:
            profile_template = yaml.safe_load(f)
        self.create_profile_from_profile_template(profile_template, profile_name)
        profiles_filepath = Path(get_flags().PROFILES_DIR) / Path("profiles.yml")
        fire_event(
            ProfileWrittenWithProjectTemplateYAML(
                name=profile_name, path=str(profiles_filepath)
            )
        )

    def ask_for_adapter_choice(self) -> str:
        """Ask the user which adapter (database) they'd like to use."""
        available_adapters = list(_get_adapter_plugin_names())

        if not available_adapters:
            raise dvt.exceptions.NoAdaptersAvailableError()

        prompt_msg = (
            "Which database would you like to use?\n"
            + "\n".join([f"[{n + 1}] {v}" for n, v in enumerate(available_adapters)])
            + "\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)"
            + "\n\nEnter a number"
        )
        numeric_choice = click.prompt(prompt_msg, type=click.INT)
        return available_adapters[numeric_choice - 1]

    def setup_profile(self, profile_name: str) -> None:
        """Set up a new profile for a project.

        Skips silently if the profile already exists in profiles.yml
        (consistent with computes.yml and buckets.yml behavior).
        """
        fire_event(SettingUpProfile())
        # Check if profile already exists - skip silently if so
        if self.check_if_profile_exists(profile_name):
            return
        # If a profile_template.yml exists in the project root, that effectively
        # overrides the profile_template.yml for the given target.
        profile_template_path = Path("profile_template.yml")
        if profile_template_path.exists():
            try:
                # This relies on a valid profile_template.yml from the user,
                # so use a try: except to fall back to the default on failure
                self.create_profile_using_project_profile_template(profile_name)
                return
            except Exception:
                fire_event(InvalidProfileTemplateYAML())
        adapter = self.ask_for_adapter_choice()
        self.create_profile_from_target(adapter, profile_name=profile_name)

    def get_valid_project_name(self) -> str:
        """Returns a valid project name, either from CLI arg or user prompt."""

        # Lazy import to avoid ModuleNotFoundError
        from dvt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME

        name = self.args.project_name
        internal_package_names = {GLOBAL_PROJECT_NAME}
        available_adapters = list(_get_adapter_plugin_names())
        for adapter_name in available_adapters:
            internal_package_names.update(f"dbt_{adapter_name}")
        while not ProjectName.is_valid(name) or name in internal_package_names:
            if name:
                click.echo(name + " is not a valid project name.")
            name = click.prompt(
                "Enter a name for your project (letters, digits, underscore)"
            )

        return name

    def create_new_project(self, project_name: str, profile_name: str):
        self.copy_starter_repo(project_name)
        os.chdir(project_name)
        with open(DBT_PROJECT_FILE_NAME, "r") as f:
            content = f"{f.read()}".format(
                project_name=project_name, profile_name=profile_name
            )
        with open(DBT_PROJECT_FILE_NAME, "w") as f:
            f.write(content)
        fire_event(
            ProjectCreated(
                project_name=project_name,
                docs_url=DOCS_URL,
                slack_url=SLACK_URL,
            )
        )

    def _is_in_project_directory(self) -> bool:
        """Check if the CURRENT directory contains a dbt/dvt project file.

        Unlike default_project_dir() which searches parents and children,
        this only checks the current working directory. This is intentional
        for 'dvt init' - we want to know if we're literally inside a project,
        not if there's a project nearby.
        """
        cwd = Path.cwd()
        return (cwd / DBT_PROJECT_FILE_NAME).is_file() or (
            cwd / DVT_PROJECT_FILE_NAME
        ).is_file()

    def run(self):
        """Entry point for the init task."""
        profiles_dir = get_flags().PROFILES_DIR
        self.create_profiles_dir(profiles_dir)
        self.create_dvt_user_config(profiles_dir)

        # Check if we're currently inside a project directory
        # Note: We check the CURRENT directory only, not parents or children.
        # This is different from other commands which use default_project_dir().
        in_project = self._is_in_project_directory()

        if in_project:
            # If --profile was specified, it means use an existing profile, which is not
            # applicable to this case
            if self.args.profile:
                raise DbtRuntimeError(
                    msg="Can not init existing project with specified profile, edit dbt_project.yml instead"
                )

            # When dvt init is run inside an existing project,
            # set up profiles/computes/buckets templates.
            profile_name = self.get_profile_name_from_current_project()
            profiles_dir = str(get_flags().PROFILES_DIR)
            # Always append computes/buckets templates for the profile
            append_profile_to_computes_yml(profile_name, profiles_dir)
            append_profile_to_buckets_yml(profile_name, profiles_dir)
            # Interactive or non-interactive profile setup
            if self.args.skip_profile_setup:
                # Add commented template to profiles.yml
                append_profile_to_profiles_yml(profile_name, profiles_dir)
            else:
                # Interactive: ask user for adapter and credentials
                self.setup_profile(profile_name)
        else:
            # When dvt init is run outside of an existing project,
            # create a new project and set up the user's profile.
            project_name = self.get_valid_project_name()
            project_path = Path(project_name)
            if project_path.exists():
                fire_event(ProjectNameAlreadyExists(name=project_name))
                return

            profiles_dir = str(get_flags().PROFILES_DIR)

            # If the user specified an existing profile to use, use it instead of generating a new one
            user_profile_name = self.args.profile
            if user_profile_name:
                if not self.check_if_profile_exists(user_profile_name):
                    raise DbtRuntimeError(
                        msg="Could not find profile named '{}'".format(
                            user_profile_name
                        )
                    )
                self.create_new_project(project_name, user_profile_name)
                # Append computes/buckets for the existing profile
                append_profile_to_computes_yml(user_profile_name, profiles_dir)
                append_profile_to_buckets_yml(user_profile_name, profiles_dir)
            else:
                profile_name = project_name
                # Create the project first to avoid leaving orphan profile if it fails
                self.create_new_project(project_name, profile_name)
                # Always append computes/buckets templates for the profile
                append_profile_to_computes_yml(profile_name, profiles_dir)
                append_profile_to_buckets_yml(profile_name, profiles_dir)
                # Interactive or non-interactive profile setup
                if self.args.skip_profile_setup:
                    # Add commented template to profiles.yml
                    append_profile_to_profiles_yml(profile_name, profiles_dir)
                else:
                    # Interactive: ask user for adapter and credentials
                    self.setup_profile(profile_name)
