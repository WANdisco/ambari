import os

from ambari_commons.constants import AMBARI_SUDO_BINARY
from resource_management import *
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import stack_tools
from resource_management.libraries.functions.fcntl_based_process_lock import FcntlBasedProcessLock

nsn_distro_root = "/opt/nsn/ngdb"
stack_version = "5.0.0.0-0"
stack_name = "wdd"


def setup_nsn_distro():
    stack_path = "/usr/" + stack_name + "/" + stack_version
    ambari_to_nsn_component_aliases = {
        "hadoop-hdfs": "hadoop",
        "hadoop-yarn": "hadoop",
        "hadoop-mapreduce": "hadoop",
        "hadoop-mapreduce-client": "hadoop-mapreduce",
        "hadoop-yarn-client": "hadoop-yarn",
        "hive-hcatalog": "hive/hcatalog",
        "spark2": "spark",
        "storm-slider-client": "slider",
        "ranger-admin": "ranger/ranger-admin",
        "ranger-kms": "ranger/ranger-kms",
        "ranger-tagsync": "ranger/ranger-tagsync",
        "ranger-usersync": "ranger/ranger-usersync",
        "ranger-atlas-plugin": "ranger/ranger-atlas-plugin",
        "ranger-hbase-plugin": "ranger/ranger-hbase-plugin",
        "ranger-hdfs-plugin": "ranger/ranger-hdfs-plugin",
        "ranger-hive-plugin": "ranger/ranger-hive-plugin",
        "ranger-kafka-plugin": "ranger/ranger-kafka-plugin",
        "ranger-knox-plugin": "ranger/ranger-knox-plugin",
        "ranger-solr-plugin": "ranger/ranger-solr-plugin",
        "ranger-storm-plugin": "ranger/ranger-storm-plugin",
        "ranger-yarn-plugin": "ranger/ranger-yarn-plugin",
        "tez_hive2": "tez"
    }

    nsn_components = ["tez", "hadoop", "zookeeper", "hbase", "hive", "slider", "sqoop", "spark", "storm", "ranger", "hive2" ]

    Directory(
        stack_path,
        create_parents=True,
        ignore_failures=True,
        not_if='ls {0}'.format(stack_path)
    )

    Logger.info("--- Stack diagnostics: ---")
    Logger.info("Is {0} exists? {1}".format(nsn_distro_root, os.path.exists(nsn_distro_root)))

    for nsn_component in nsn_components:
        nsn_ambari_alias = os.path.join(stack_path, nsn_component)
        nsn_component_path = os.path.join(nsn_distro_root, nsn_component)
        Directory(
            nsn_component_path,
            action="create",
            create_parents=True,
            not_if="test -d {dir}".format(dir=nsn_component_path)
        )
        Link(
            nsn_ambari_alias,
            to=nsn_component_path,
            ignore_failures=True,
            not_if='ls {0}'.format(nsn_ambari_alias)
        )

    for ambari_component, nsn_component in ambari_to_nsn_component_aliases.iteritems():
        ambari_component_path = os.path.join(stack_path, ambari_component)
        nsn_ambari_alias = os.path.join(stack_path, nsn_component)
        if "/" in nsn_component:
            # special case - subdirectory reference - create it
            Directory(
                nsn_ambari_alias,
                action="create",
                create_parents=True,
                not_if="test -d {dir}".format(dir=nsn_ambari_alias)
            )
        Link(
            ambari_component_path,
            to=nsn_ambari_alias,
            ignore_failures=True,
            not_if='ls {0}'.format(ambari_component_path)
        )


def setup_stack_symlinks():
    """
    Invokes <stack-selector-tool> set all against a calculated fully-qualified, "normalized" version based on a
    stack version, such as "2.3". This should always be called after a component has been
    installed to ensure that all WDD pointers are correct. The stack upgrade logic does not
    interact with this since it's done via a custom command and will not trigger this hook.
    :return:
    """
    config = Script.get_config()
    tmp_dir = Script.get_tmp_dir()
    is_parallel_execution_enabled = int(default("/agentConfigParams/agent/parallel_execution", 0)) == 1

    stack_version_unformatted = config['hostLevelParams']['stack_version']
    stack_version_formatted = format_stack_version(stack_version_unformatted)
    current_version = default("/hostLevelParams/current_version", None)
    upgrade_suspended = default("/roleParams/upgrade_suspended", False)
    stack_select_lock_file = os.path.join(tmp_dir, "stack_select_lock_file")

    if stack_version_formatted != "" and compare_versions(stack_version_formatted, '2.2') >= 0:
        # try using the exact version first, falling back in just the stack if it's not defined
        # which would only be during an intial cluster installation
        version = current_version if current_version is not None else stack_version_unformatted

        if not upgrade_suspended:
            # On parallel command execution this should be executed by a single process at a time.
            with FcntlBasedProcessLock(stack_select_lock_file, enabled=is_parallel_execution_enabled,
                                       skip_fcntl_failures=True):
                select_stack_for_all(version)


def select_stack_for_all(version_to_select):
    sudo = AMBARI_SUDO_BINARY
    stack_root = Script.get_stack_root()
    (stack_selector_name, stack_selector_path, stack_selector_package) = stack_tools.get_stack_tool(
        stack_tools.STACK_SELECTOR_NAME)
    # it's an error, but it shouldn't really stop anything from working
    if version_to_select is None:
        Logger.error(
            format("Unable to execute {stack_selector_name} after installing because there was no version specified"))
        return

    Logger.info("Executing {0} set all on {1}".format(stack_selector_name, version_to_select))
    Logger.info("Sudo: {0}, stack root: {1}".format(sudo, stack_root))

    command = format(
        '{sudo} {stack_selector_path} set all `ambari-python-wrap {stack_selector_path} versions | grep ^{version_to_select} | tail -1`')
    only_if_command = format('ls -d {stack_root}/{version_to_select}*')
    Execute(command, only_if=only_if_command)


def setup_stack_config_dirs():
    for package_name, directories in conf_select.get_package_dirs().iteritems():
        conf_selector_name = stack_tools.get_stack_tool_name(stack_tools.CONF_SELECTOR_NAME)
        Logger.info("Selecting version for config of package {0}".format(package_name))
        Logger.info("The current cluster stack of {0} does not require backing up configurations; "
                    "only {1} versioned config directories will be created.".format(stack_version, conf_selector_name))
        # only link configs for all known packages
        conf_select.select(stack_name, package_name, stack_version, ignore_errors=True)


def setup():
    setup_nsn_distro()
    setup_stack_symlinks()
    setup_stack_config_dirs()
