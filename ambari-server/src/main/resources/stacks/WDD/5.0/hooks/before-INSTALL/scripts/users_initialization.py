from copy import copy

import collections
from resource_management import Group, User, Logger, Directory, StaticFile, File, Execute, default, Script, re
# simplejson is much faster comparing to Python 2.6 json module and has the same functions set.
import ambari_simplejson as json


def setup_users():
    """
    Creates users before cluster installation
    """
    config = Script.get_config()
    user_list = json.loads(config['hostLevelParams']['user_list'])
    group_list = json.loads(config['hostLevelParams']['group_list'])
    host_sys_prepped = default("/hostLevelParams/host_sys_prepped", False)

    ignore_groupsusers_create = default("/configurations/cluster-env/ignore_groupsusers_create", False)
    fetch_nonlocal_groups = config['configurations']['cluster-env']["fetch_nonlocal_groups"]
    override_uid = str(default("/configurations/cluster-env/override_uid", "true")).lower()

    ganglia_server_hosts = default("/clusterHostInfo/ganglia_server_host", [])
    namenode_host = default("/clusterHostInfo/namenode_host", [])
    hbase_master_hosts = default("/clusterHostInfo/hbase_master_hosts", [])
    oozie_servers = default("/clusterHostInfo/oozie_server", [])
    falcon_server_hosts = default("/clusterHostInfo/falcon_server_hosts", [])
    ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
    spark_jobhistoryserver_hosts = default("/clusterHostInfo/spark_jobhistoryserver_hosts", [])

    has_namenode = not len(namenode_host) == 0
    has_ganglia_server = not len(ganglia_server_hosts) == 0
    has_tez = 'tez-site' in config['configurations']
    has_hbase_masters = not len(hbase_master_hosts) == 0
    has_oozie_server = not len(oozie_servers) == 0
    has_falcon_server_hosts = not len(falcon_server_hosts) == 0
    has_ranger_admin = not len(ranger_admin_hosts) == 0
    has_spark = not len(spark_jobhistoryserver_hosts) == 0

    smoke_user_dirs = format(
        "/tmp/hadoop-{smoke_user},/tmp/hsperfdata_{smoke_user},/home/{smoke_user},/tmp/{smoke_user},/tmp/sqoop-{smoke_user}")
    hbase_tmp_dir = "/tmp/hbase-hbase"

    proxyuser_group = default("/configurations/hadoop-env/proxyuser_group", "users")
    ranger_group = config['configurations']['ranger-env']['ranger_group']
    spark_group = config['configurations']['spark-env']['spark_group']
    dfs_cluster_administrators_group = config['configurations']['hdfs-site']["dfs.cluster.administrators"]

    # users and groups
    hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
    hbase_user = config['configurations']['hbase-env']['hbase_user']
    smoke_user = config['configurations']['cluster-env']['smokeuser']
    gmetad_user = config['configurations']['ganglia-env']["gmetad_user"]
    gmond_user = config['configurations']['ganglia-env']["gmond_user"]
    tez_user = config['configurations']['tez-env']["tez_user"]
    oozie_user = config['configurations']['oozie-env']["oozie_user"]
    falcon_user = config['configurations']['falcon-env']["falcon_user"]
    ranger_user = config['configurations']['ranger-env']["ranger_user"]
    spark_user = config['configurations']['spark-env']['spark_user']

    user_group = config['configurations']['cluster-env']['user_group']

    user_to_groups_dict = collections.defaultdict(lambda: [user_group])
    user_to_groups_dict[smoke_user] = [proxyuser_group]
    if has_ganglia_server:
        user_to_groups_dict[gmond_user] = [gmond_user]
        user_to_groups_dict[gmetad_user] = [gmetad_user]
    if has_tez:
        user_to_groups_dict[tez_user] = [proxyuser_group]
    if has_oozie_server:
        user_to_groups_dict[oozie_user] = [proxyuser_group]
    if has_falcon_server_hosts:
        user_to_groups_dict[falcon_user] = [proxyuser_group]
    if has_ranger_admin:
        user_to_groups_dict[ranger_user] = [ranger_group]
    if has_spark:
        user_to_groups_dict[spark_user] = [spark_group]

    user_to_gid_dict = collections.defaultdict(lambda: user_group)
    should_create_users_and_groups = not host_sys_prepped and not ignore_groupsusers_create
    tez_am_view_acls = default("/configurations/tez-site/tez.am.view-acls", "*")

    def create_tez_am_view_acls():
        """
        tez.am.view-acls support format <comma-delimited list of usernames><space><comma-delimited list of group names>
        """
        if not tez_am_view_acls.startswith("*"):
            create_users_and_groups(tez_am_view_acls, fetch_nonlocal_groups)

    def create_dfs_cluster_admins():
        """
        dfs.cluster.administrators support format <comma-delimited list of usernames><space><comma-delimited list of group names>
        """
        groups_list = create_users_and_groups(dfs_cluster_administrators_group, fetch_nonlocal_groups)

        User(hdfs_user,
             groups=user_to_groups_dict[hdfs_user] + groups_list,
             fetch_nonlocal_groups=fetch_nonlocal_groups
             )

    if should_create_users_and_groups:
        for group in group_list:
            Group(group)

        for user in user_list:
            User(user,
                 gid=user_to_gid_dict[user],
                 groups=user_to_groups_dict[user],
                 fetch_nonlocal_groups=fetch_nonlocal_groups
                 )

        if override_uid == "true":
            set_uid(smoke_user, smoke_user_dirs, ignore_groupsusers_create)
        else:
            Logger.info('Skipping setting uid for smoke user as host is sys prepped')
    else:
        Logger.info(
            'Skipping creation of User and Group as host is sys prepped or ignore_groupsusers_create flag is on')
        pass

    if has_hbase_masters:
        hbase_user_dirs = format(
            "/home/{hbase_user},/tmp/{hbase_user},/usr/bin/{hbase_user},/var/log/{hbase_user},{hbase_tmp_dir}")
        Directory(hbase_tmp_dir,
                  owner=hbase_user,
                  mode=0775,
                  create_parents=True,
                  cd_access="a",
                  )
        if not host_sys_prepped and override_uid == "true":
            set_uid(hbase_user, hbase_user_dirs, ignore_groupsusers_create)
        else:
            Logger.info('Skipping setting uid for hbase user as host is sys prepped')
            pass

    if not host_sys_prepped:
        if has_namenode and should_create_users_and_groups:
            create_dfs_cluster_admins()
        if has_tez and should_create_users_and_groups:
            create_tez_am_view_acls()
    else:
        Logger.info('Skipping setting dfs cluster admin and tez view acls as host is sys prepped')


def create_users_and_groups(user_and_groups, fetch_nonlocal_groups):
    parts = re.split('\s', user_and_groups)
    if len(parts) == 1:
        parts.append("")

    users_list = parts[0].split(",") if parts[0] else []
    groups_list = parts[1].split(",") if parts[1] else []

    if users_list:
        User(users_list,
             fetch_nonlocal_groups=fetch_nonlocal_groups)

    if groups_list:
        Group(copy(groups_list))
    return groups_list


def set_uid(user, user_dirs, ignore_groupsusers_create):
    """
    user_dirs - comma separated directories
    """
    tmp_dir = Script.get_tmp_dir()
    File("{0}/changeUid.sh".format(tmp_dir),
         content=StaticFile("changeToSecureUid.sh"),
         mode=0555)
    ignore_groupsusers_create_str = str(ignore_groupsusers_create).lower()
    Execute("{0}/changeUid.sh {1} {2}".format(tmp_dir, user, user_dirs),
            not_if="(test $(id -u {0}) -gt 1000) || ({1})".format(user, ignore_groupsusers_create_str))
