from resource_management import *
from resource_management.libraries.functions import conf_select

import os
import repo_initialization
import users_initialization
import java_installer
import wdd_stack


def fix_tmp_dir_permissions():
  config = Script.get_config()
  tmp_dir = Script.get_tmp_dir()
  hadoop_java_io_tmpdir = os.path.join(tmp_dir, "hadoop_java_io_tmpdir")
  hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
  user_group = config['configurations']['cluster-env']['user_group']

  Directory(hadoop_java_io_tmpdir,
            owner=hdfs_user,
            group=user_group,
            mode=01777
            )


class BeforeInstallHook(Hook):

  def hook(self, env):
    import params
    # this is required in order for format() function from ambari to pick up variables from params module
    env.set_params(params)

    fix_tmp_dir_permissions()

    repo_initialization.install_repos()
    if not os.path.isfile("/opt/nsn/ngdb/wdd/wdd-select"):
      Package("wdd-select")

    wdd_stack.setup()
    users_initialization.setup_users()
    java_installer.setup_java()

    Directory(
      Script.get_stack_root(),
      create_parents=True
    )

    Logger.info("--- Stack diagnostics: ---")
    Logger.info("Stack root: {0}".format(Script.get_stack_root()))
    Logger.info("Stack version: {0}".format(Script.get_stack_version()))
    Logger.info("In stack upgrade: {0}".format(Script.in_stack_upgrade()))
    Logger.info("Hadoop conf dir is: {0}".format(conf_select.get_hadoop_conf_dir()))


if __name__ == "__main__":
  BeforeInstallHook().execute()
