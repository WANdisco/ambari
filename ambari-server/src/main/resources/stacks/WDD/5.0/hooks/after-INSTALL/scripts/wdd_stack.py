import os

from ambari_commons.constants import AMBARI_SUDO_BINARY
from resource_management import *
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import stack_tools
from resource_management.libraries.functions.fcntl_based_process_lock import FcntlBasedProcessLock

def get_real_stack_version(stack_selector_path, formatted_version):
    command = format("ambari-python-wrap {stack_selector_path} versions | grep ^{formatted_version} | tail -1")
    return_code, stack_output = shell.call(command, timeout=20)
    return stack_output.strip()

def get_wdd_stack_version():
    config = Script.get_config()
    stack_version_unformatted = config['hostLevelParams']['stack_version']
    current_version = default("/hostLevelParams/current_version", None)
    version = current_version if current_version is not None else stack_version_unformatted
    version_formatted = format_stack_version(version)
    (stack_selector_name, stack_selector_path, stack_selector_package) = \
        stack_tools.get_stack_tool(stack_tools.STACK_SELECTOR_NAME)
    return get_real_stack_version(stack_selector_path, version_formatted)

def setup_stack_config_dirs():
    stack_name = Script.get_stack_name()
    stack_version = get_wdd_stack_version()
    for package_name, directories in conf_select.get_package_dirs().iteritems():
        conf_selector_name = stack_tools.get_stack_tool_name(stack_tools.CONF_SELECTOR_NAME)
        Logger.info("Selecting version for config of package {0}".format(package_name))
        Logger.info("Package directories: {0}".format(directories))
        Logger.info("The current cluster stack of {0} does not require backing up configurations; "
                    "only {1} versioned config directories will be created.".format(stack_version, conf_selector_name))
        # only link configs for all known packages
        conf_select.select(stack_name, package_name, stack_version, ignore_errors=True)


def setup():
    setup_stack_config_dirs()
