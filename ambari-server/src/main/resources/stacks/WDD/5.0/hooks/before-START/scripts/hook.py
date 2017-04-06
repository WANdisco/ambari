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
    create_ranger_hbase_symlinks()
    create_ranger_hive_symlinks()
    create_ranger_storm_symlinks()
    create_ranger_yarn_symlinks()


def create_javahome_symlink():
    if os.path.exists("/usr/jdk/jdk1.6.0_31") and not os.path.exists("/usr/jdk64/jdk1.6.0_31"):
        Directory("/usr/jdk64/",
                  create_parents=True,
                  )
        Link("/usr/jdk/jdk1.6.0_31",
             to="/usr/jdk64/jdk1.6.0_31",
             )

def create_ranger_hbase_symlinks():
    import params
    ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
    has_ranger_admin = not len(ranger_admin_hosts) == 0
    if has_ranger_admin:
        if params.has_hbase_masters :
            from resource_management.libraries.functions.setup_ranger_plugin_xml import setup_ranger_plugin_jar_symblink
            stack_version = get_stack_version('hadoop-client')
            stack_root = Script.get_stack_root()
            if(os.path.exists("/usr/wdd/current/hbase-regionserver/lib")) :
              setup_ranger_plugin_jar_symblink(stack_version, 'hbase', component_list=['hbase-regionserver'])

def create_ranger_hive_symlinks():
    import params
    ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
    has_ranger_admin = not len(ranger_admin_hosts) == 0
    if has_ranger_admin:
        if params.has_hive_server_host :
            from resource_management.libraries.functions.setup_ranger_plugin_xml import setup_ranger_plugin_jar_symblink
            stack_version = get_stack_version('hadoop-client')
            #create symlinks to plugin libs only in case hive server installed on host
            if(os.path.isdir("/usr/wdd/current/hive-server2/lib")) :
              setup_ranger_plugin_jar_symblink(stack_version, 'hive', component_list=['hive-server2'])

def create_ranger_storm_symlinks():
    import params
    ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
    has_ranger_admin = not len(ranger_admin_hosts) == 0
    if has_ranger_admin:
        if params.has_nimbus_host :
            from resource_management.libraries.functions.setup_ranger_plugin_xml import setup_ranger_plugin_jar_symblink
            stack_version = get_stack_version('hadoop-client')
            #create symlinks to plugin libs only in case storm nimbus installed on host
            if(os.path.isdir("/usr/wdd/current/storm-nimbus/lib")) :
              setup_ranger_plugin_jar_symblink(stack_version, 'storm', component_list=['storm-nimbus'])

def create_ranger_yarn_symlinks():
    import params
    ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
    has_ranger_admin = not len(ranger_admin_hosts) == 0
    if has_ranger_admin:
        config = Script.get_config()
        is_supported_yarn_ranger = config['configurations']['yarn-env']['is_supported_yarn_ranger']
        if params.has_resourcemanager and is_supported_yarn_ranger :
            from resource_management.libraries.functions.setup_ranger_plugin_xml import setup_ranger_plugin_jar_symblink
            stack_version = get_stack_version('hadoop-client')
            #create symlinks to plugin libs only in case resourcemanager installed on host and ranger is enabled for yarn
            if(os.path.isdir("/usr/wdd/current/hadoop-yarn-resourcemanager/lib") and not os.path.exists("/usr/wdd/current/hadoop-yarn-resourcemanager/lib/ranger-yarn-plugin-impl")) :
              Logger.info("Create ranger symlinks for hadoop-yarn-resourcemanager")
              setup_ranger_plugin_jar_symblink(stack_version, 'yarn', component_list=['hadoop-client'])


def copy_container_executor_cfg():
    import params
    destination = format("{hadoop_home}/etc/hadoop/container-executor.cfg")

    Link(os.path.join(params.hadoop_conf_dir, "container-executor.cfg"),
        to=destination,
        )

class BeforeStartHook(Hook):
    def hook(self, env):
        import params
        env.set_params(params)

        create_javahome_symlink()
        setup_hadoop_env()
        copy_container_executor_cfg()
        create_topology_script_and_mapping()


if __name__ == "__main__":
    BeforeStartHook().execute()
