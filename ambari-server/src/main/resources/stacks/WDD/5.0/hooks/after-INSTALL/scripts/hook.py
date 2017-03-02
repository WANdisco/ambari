import os
from resource_management import *

nsn_distro_root = "/opt/nsn/ngdb"
wdd_stack_version = "5.0.0.0-0"


def setup_stack_log_dirs():
    Link(
        nsn_distro_root + "/hadoop/logs",
        to="/var/log/hadoop/hdfs",
        ignore_failures=True
    )


def link_config_scripts():
    current_stack_root = "/usr/wdd/current"
    config_script_mappings = {
        "/usr/libexec/hadoop-config.sh": "/hadoop-client/libexec/hadoop-config.sh",
        "/usr/libexec/hdfs-config.sh": "/hadoop-client/libexec/hdfs-config.sh",
        "/usr/libexec/mapred-config.sh": "/hadoop-client/libexec/mapred-config.sh",
        "/usr/libexec/yarn-config.sh": "/hadoop-yarn-client/libexec/yarn-config.sh"
    }

    for system_script_path, stack_symlink in config_script_mappings.iteritems():
        stack_script_path = current_stack_root + stack_symlink
        Link(
            system_script_path,
            to=stack_script_path,
            ignore_failures=True,
            not_if='ls {0}'.format(system_script_path)
        )


def patch_hadoop_config():
    hadoop_dir = os.path.join(nsn_distro_root, "hadoop")
    hadoop_yarn_apps_distributedshell = os.path.join(hadoop_dir, "hadoop-yarn-applications-distributedshell.jar")
    if os.path.isdir(hadoop_dir) and not os.path.exists(hadoop_yarn_apps_distributedshell):
        File(os.path.join(hadoop_dir, "libexec", "hadoop-config.sh"),
             content=StaticFile('hadoop-config.sh'),
             mode=0755)
        File(hadoop_yarn_apps_distributedshell,
             content=StaticFile('hadoop-yarn-applications-distributedshell.jar'),
             mode=0755)
        Link(os.path.join(hadoop_dir, "hadoop-mapreduce-examples-2.7.3.jar"),
             to=nsn_distro_root + "/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar",
             not_if='ls {0}'.format(nsn_distro_root + "/hadoop/hadoop-mapreduce-examples-2.7.3.jar")
             )


def setup_user_environment():
    Link("/usr/bin/hdfs",
         to="/usr/wdd/current/hadoop-client/bin/hdfs",
         not_if='ls {0}'.format("/usr/bin/hdfs")
         )


def build_framework_tarballs(config):
    frameworks = {
        "tez": (nsn_distro_root + "/tez", nsn_distro_root + "/tez/lib/tez.tar.gz"),
        "mapreduce": (nsn_distro_root + "/hadoop", nsn_distro_root + "/hadoop/mapreduce.tar.gz"),
    }

    def make_framework_tarball(framework_name, framework_file_path, framework_path):
        build_framework_script = format("""
            (tar -czvf {framework_file_path} -C {framework_path} ./ --exclude {framework_file_path} --warning=no-file-changed);
            if [ $? -eq 1 ]; then (exit 0); fi;
        """)  # 1 means directory changed while we were making an archive
        ExecuteScript("build_{0}_framework".format(framework_name),
                      code=build_framework_script,
                      only_if=format("test -d {framework_path}"),
                      not_if=format("test -f {framework_file_path}"),
                      user="root")

    for framework_name, (framework_path, framework_tarball) in frameworks.iteritems():
        Logger.info("Building framework tarball for {0} in file {1}".format(framework_name, framework_tarball))
        make_framework_tarball(framework_name, framework_tarball, framework_path)

def create_spark_tmp_dir():
    """
    Create folder /tmp/spark-events directory in order to support default FsHistoryProvider in spark
    instead of YarnHistoryProvider from HDP stack that writing files to HDFS directly
    """
    config = Script.get_config()
    spark_jobhistoryserver_hosts = default("/clusterHostInfo/spark_jobhistoryserver_hosts", [])
    if len(spark_jobhistoryserver_hosts) > 0:
        spark_user = config['configurations']['spark-env']['spark_user']
        Directory(
            "/tmp/spark-events",
            mode=0755,
            owner=spark_user,
            group='hadoop'
        )

class AfterInstallHook(Hook):
    def hook(self, env):
        setup_stack_log_dirs()
        link_config_scripts()
        patch_hadoop_config()
        create_spark_tmp_dir()
        build_framework_tarballs(Script.get_config())

        # workaround inaccessible /var/hdfs directory where we suggest to write data to
        hadoop_user = default("configurations/cluster-env/hadoop.user.name", "hdfs")

        Directory(
            "/var/hdfs",
            mode=0755,
            owner=hadoop_user
        )


if __name__ == "__main__":
    AfterInstallHook().execute()
