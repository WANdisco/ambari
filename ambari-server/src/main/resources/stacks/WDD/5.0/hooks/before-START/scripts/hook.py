import os
import sys
from resource_management import *
from rack_awareness import create_topology_script_and_mapping


def setup_hadoop_env():
    """
        We have to do this here because for some reason hadoop service doesn't do this, and hdp also
        creates hadoop-env.sh from similar hook.
    """
    import params

    Logger.info("Java home: {0}, jdk location: {1}".format(params.java_home, params.jdk_location))
    if params.has_namenode:
        if params.security_enabled:
            tc_owner = "root"
        else:
            tc_owner = params.hdfs_user
        Directory(params.hadoop_conf_empty_dir,
                  create_parents=True,
                  owner='root',
                  group='root'
                  )
        Link(params.hadoop_conf_dir,
             to=params.hadoop_conf_empty_dir,
             not_if=format("ls {hadoop_conf_dir}")
             )
        File(os.path.join(params.hadoop_conf_dir, 'hadoop-env.sh'),
             owner=tc_owner,
             content=InlineTemplate(params.hadoop_env_sh_template)
             )


def create_javahome_symlink():
    if os.path.exists("/usr/jdk/jdk1.6.0_31") and not os.path.exists("/usr/jdk64/jdk1.6.0_31"):
        Directory("/usr/jdk64/",
                  create_parents=True,
                  )
        Link("/usr/jdk/jdk1.6.0_31",
             to="/usr/jdk64/jdk1.6.0_31",
             )


class BeforeStartHook(Hook):
    def hook(self, env):
        import params
        env.set_params(params)

        create_javahome_symlink()
        setup_hadoop_env()
        create_topology_script_and_mapping()


if __name__ == "__main__":
    BeforeStartHook().execute()
