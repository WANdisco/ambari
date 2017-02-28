from ambari_commons.constants import AMBARI_SUDO_BINARY
from resource_management.libraries.functions.expect import expect
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import default

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

# java installer config
sudo = AMBARI_SUDO_BINARY

artifact_dir = format("{tmp_dir}/AMBARI-artifacts/")
jdk_name = default("/hostLevelParams/jdk_name", None)
java_home = config['hostLevelParams']['java_home']
java_version = expect("/hostLevelParams/java_version", int)
jdk_location = config['hostLevelParams']['jdk_location']

# general
host_sys_prepped = default("/hostLevelParams/host_sys_prepped", False)


# repo templates
repo_rhel_suse = config['configurations']['cluster-env']['repo_suse_rhel_template']
repo_ubuntu = config['configurations']['cluster-env']['repo_ubuntu_template']
repo_info = config['hostLevelParams']['repo_info']
service_repo_info = default("/hostLevelParams/service_repo_info", None)