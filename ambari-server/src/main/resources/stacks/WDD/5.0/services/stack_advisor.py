#!/usr/bin/env ambari-python-wrap

import imp
import os
import re
import socket
import traceback
import sys

from math import ceil, floor

from resource_management.core.logger import Logger
from resource_management.libraries.functions.mounted_dirs_helper import get_mounts_with_multiple_data_dirs
from resource_management.libraries.functions.get_bare_principal import get_bare_principal
from urlparse import urlparse

from stack_advisor import DefaultStackAdvisor


class WDD50StackAdvisor(DefaultStackAdvisor):
    def __init__(self):
        super(WDD50StackAdvisor, self).__init__()
        Logger.initialize_logger()

    def getComponentLayoutValidations(self, services, hosts):
        """Returns array of Validation objects about issues with hostnames components assigned to"""
        items = super(WDD50StackAdvisor, self).getComponentLayoutValidations(services, hosts)

        # Validating NAMENODE and SECONDARY_NAMENODE are on different hosts if possible
        # Use a set for fast lookup
        hostsSet =  set(super(WDD50StackAdvisor, self).getActiveHosts([host["Hosts"] for host in hosts["items"]]))  #[host["Hosts"]["host_name"] for host in hosts["items"]]
        hostsCount = len(hostsSet)

        componentsListList = [service["components"] for service in services["services"]]
        componentsList = [item for sublist in componentsListList for item in sublist]
        nameNodeHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "NAMENODE"]
        secondaryNameNodeHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "SECONDARY_NAMENODE"]

        # Validating cardinality
        for component in componentsList:
            if component["StackServiceComponents"]["cardinality"] is not None:
                componentName = component["StackServiceComponents"]["component_name"]
                componentDisplayName = component["StackServiceComponents"]["display_name"]
                componentHosts = []
                if component["StackServiceComponents"]["hostnames"] is not None:
                    componentHosts = [componentHost for componentHost in component["StackServiceComponents"]["hostnames"] if componentHost in hostsSet]
                componentHostsCount = len(componentHosts)
                cardinality = str(component["StackServiceComponents"]["cardinality"])
                Logger.info("Component {0}, displayName: {1}, cardinality: {2}, hosts: {3}".format(componentName, componentDisplayName, cardinality, componentHosts))
                # cardinality types: null, 1+, 1-2, 1, ALL
                message = None
                if "+" in cardinality:
                    hostsMin = int(cardinality[:-1])
                    if componentHostsCount < hostsMin:
                        message = "At least {0} {1} components should be installed in cluster.".format(hostsMin, componentDisplayName)
                elif "-" in cardinality:
                    nums = cardinality.split("-")
                    hostsMin = int(nums[0])
                    hostsMax = int(nums[1])
                    if componentHostsCount > hostsMax or componentHostsCount < hostsMin:
                        message = "Between {0} and {1} {2} components should be installed in cluster.".format(hostsMin, hostsMax, componentDisplayName)
                elif "ALL" == cardinality:
                    if componentHostsCount != hostsCount:
                        message = "{0} component should be installed on all hosts in cluster.".format(componentDisplayName)
                else:
                    if componentHostsCount != int(cardinality):
                        message = "Exactly {0} {1} components should be installed in cluster.".format(int(cardinality), componentDisplayName)

                if message is not None:
                    items.append({"type": 'host-component', "level": 'ERROR', "message": message, "component-name": componentName})

        # Validating host-usage
        usedHostsListList = [component["StackServiceComponents"]["hostnames"] for component in componentsList if not self.isComponentNotValuable(component)]
        usedHostsList = [item for sublist in usedHostsListList for item in sublist]
        nonUsedHostsList = [item for item in hostsSet if item not in usedHostsList]
        for host in nonUsedHostsList:
            items.append( { "type": 'host-component', "level": 'ERROR', "message": 'Host is not used', "host": str(host) } )

        return items

    def getServiceConfigurationRecommenderDict(self):
        return {
            "YARN": self.recommendYARNConfigurations,
            "MAPREDUCE2": self.recommendMapReduce2Configurations,
            "HDFS": self.recommendHDFSConfigurations,
            "HBASE": self.recommendHbaseConfigurations,
            "STORM": self.recommendStormConfigurations,
            "AMBARI_METRICS": self.recommendAmsConfigurations,
            "RANGER": self.recommendRangerConfigurations,
            "RANGER_KMS": self.recommendRangerKMSConfigurations,
            "HIVE": self.recommendHiveConfigurations,
        }

    def recommendYARNConfigurations(self, configurations, clusterData, services, hosts):
        putYarnProperty = self.putProperty(configurations, "yarn-site", services)
        putYarnPropertyAttribute = self.putPropertyAttribute(configurations, "yarn-site")
        putYarnEnvProperty = self.putProperty(configurations, "yarn-env", services)

        putYarnProperty("yarn.nodemanager.local-dirs", "/var/yarn/local")
        putYarnProperty("yarn.nodemanager.log-dirs", "/var/yarn/log")
        # for debug:
        putYarnProperty("yarn.nodemanager.delete.debug-delay-sec", "60000")

        nodemanagerMinRam = 1048576 # 1TB in mb
        if "referenceNodeManagerHost" in clusterData:
            nodemanagerMinRam = min(clusterData["referenceNodeManagerHost"]["total_mem"]/1024, nodemanagerMinRam)
        putYarnProperty('yarn.nodemanager.resource.memory-mb', int(round(min(clusterData['containers'] * clusterData['ramPerContainer'], nodemanagerMinRam))))
        putYarnProperty('yarn.scheduler.minimum-allocation-mb', int(clusterData['ramPerContainer']))
        putYarnProperty('yarn.scheduler.maximum-allocation-mb', int(configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.memory-mb"]))
        putYarnEnvProperty('min_user_id', self.get_system_min_uid())

        sc_queue_name = self.recommendYarnQueue(services, "yarn-env", "service_check.queue.name")
        if sc_queue_name is not None:
            putYarnEnvProperty("service_check.queue.name", sc_queue_name)

        containerExecutorGroup = 'hadoop'
        if 'cluster-env' in services['configurations'] and 'user_group' in services['configurations']['cluster-env']['properties']:
            containerExecutorGroup = services['configurations']['cluster-env']['properties']['user_group']
        putYarnProperty("yarn.nodemanager.linux-container-executor.group", containerExecutorGroup)

        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        if "TEZ" in servicesList:
            ambari_user = self.getAmbariUser(services)
            ambariHostName = socket.getfqdn()
            putYarnProperty("yarn.timeline-service.http-authentication.proxyuser.{0}.hosts".format(ambari_user), ambariHostName)
            putYarnProperty("yarn.timeline-service.http-authentication.proxyuser.{0}.groups".format(ambari_user), "*")
            old_ambari_user = self.getOldAmbariUser(services)
            if old_ambari_user is not None:
                putYarnPropertyAttribute("yarn.timeline-service.http-authentication.proxyuser.{0}.hosts".format(old_ambari_user), 'delete', 'true')
                putYarnPropertyAttribute("yarn.timeline-service.http-authentication.proxyuser.{0}.groups".format(old_ambari_user), 'delete', 'true')

        if "ranger-env" in services["configurations"] and "ranger-yarn-plugin-properties" in services[
            "configurations"] and \
                        "ranger-yarn-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            putYarnRangerPluginProperty = self.putProperty(configurations, "ranger-yarn-plugin-properties", services)
            rangerEnvYarnPluginProperty = services["configurations"]["ranger-env"]["properties"][
                "ranger-yarn-plugin-enabled"]
            putYarnRangerPluginProperty("ranger-yarn-plugin-enabled", rangerEnvYarnPluginProperty)
        rangerPluginEnabled = ''
        if 'ranger-yarn-plugin-properties' in configurations and 'ranger-yarn-plugin-enabled' in \
                configurations['ranger-yarn-plugin-properties']['properties']:
            rangerPluginEnabled = configurations['ranger-yarn-plugin-properties']['properties'][
                'ranger-yarn-plugin-enabled']
        elif 'ranger-yarn-plugin-properties' in services['configurations'] and 'ranger-yarn-plugin-enabled' in \
                services['configurations']['ranger-yarn-plugin-properties']['properties']:
            rangerPluginEnabled = services['configurations']['ranger-yarn-plugin-properties']['properties'][
                'ranger-yarn-plugin-enabled']

        if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
            putYarnProperty('yarn.acl.enable', 'true')
            putYarnProperty('yarn.authorization-provider',
                                'org.apache.ranger.authorization.yarn.authorizer.RangerYarnAuthorizer')
        else:
            putYarnPropertyAttribute('yarn.authorization-provider', 'delete', 'true')

        if 'yarn-site' in services["configurations"] and 'yarn.resourcemanager.proxy-user-privileges.enabled' in \
                services["configurations"]["yarn-site"]["properties"]:
            if self.isSecurityEnabled(services):
                # enable proxy-user privileges for secure clusters for long-running services (spark streaming etc)
                putYarnProperty('yarn.resourcemanager.proxy-user-privileges.enabled', 'true')
                if 'RANGER_KMS' in servicesList:
                    # disable proxy-user privileges on secure clusters as it does not work with TDE
                    putYarnProperty('yarn.resourcemanager.proxy-user-privileges.enabled', 'false')
            else:
                putYarnProperty('yarn.resourcemanager.proxy-user-privileges.enabled', 'false')

    def recommendMapReduce2Configurations(self, configurations, clusterData, services, hosts):
        putMapredProperty = self.putProperty(configurations, "mapred-site", services)
        putMapredProperty('yarn.app.mapreduce.am.resource.mb', int(clusterData['amMemory']))
        putMapredProperty('yarn.app.mapreduce.am.command-opts', "-Xmx" + str(int(round(0.8 * clusterData['amMemory']))) + "m")
        putMapredProperty('mapreduce.map.memory.mb', clusterData['mapMemory'])
        putMapredProperty('mapreduce.reduce.memory.mb', int(clusterData['reduceMemory']))
        putMapredProperty('mapreduce.map.java.opts', "-Xmx" + str(int(round(0.8 * clusterData['mapMemory']))) + "m")
        putMapredProperty('mapreduce.reduce.java.opts', "-Xmx" + str(int(round(0.8 * clusterData['reduceMemory']))) + "m")
        putMapredProperty('mapreduce.task.io.sort.mb', min(int(round(0.4 * clusterData['mapMemory'])), 1024))
        mr_queue = self.recommendYarnQueue(services, "mapred-site", "mapreduce.job.queuename")
        if mr_queue is not None:
            putMapredProperty("mapreduce.job.queuename", mr_queue)

    def getAmbariUser(self, services):
        ambari_user = services['ambari-server-properties']['ambari-server.user']
        if "cluster-env" in services["configurations"] \
                and "ambari_principal_name" in services["configurations"]["cluster-env"]["properties"] \
                and "security_enabled" in services["configurations"]["cluster-env"]["properties"] \
                and services["configurations"]["cluster-env"]["properties"]["security_enabled"].lower() == "true":
            ambari_user = services["configurations"]["cluster-env"]["properties"]["ambari_principal_name"]
            ambari_user = ambari_user.split('@')[0]
        return ambari_user

    def getOldAmbariUser(self, services):
        ambari_user = None
        if "cluster-env" in services["configurations"]:
            if "security_enabled" in services["configurations"]["cluster-env"]["properties"] \
                    and services["configurations"]["cluster-env"]["properties"]["security_enabled"].lower() == "true":
                ambari_user = services['ambari-server-properties']['ambari-server.user']
            elif "ambari_principal_name" in services["configurations"]["cluster-env"]["properties"]:
                ambari_user = services["configurations"]["cluster-env"]["properties"]["ambari_principal_name"]
                ambari_user = ambari_user.split('@')[0]
        return ambari_user

    def recommendAmbariProxyUsersForHDFS(self, services, servicesList, putCoreSiteProperty, putCoreSitePropertyAttribute):
        if "HDFS" in servicesList:
            ambari_user = self.getAmbariUser(services)
            ambariHostName = socket.getfqdn()
            putCoreSiteProperty("hadoop.proxyuser.{0}.hosts".format(ambari_user), ambariHostName)
            putCoreSiteProperty("hadoop.proxyuser.{0}.groups".format(ambari_user), "*")
            old_ambari_user = self.getOldAmbariUser(services)
            if old_ambari_user is not None:
                putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.hosts".format(old_ambari_user), 'delete', 'true')
                putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.groups".format(old_ambari_user), 'delete', 'true')

    def recommendHadoopProxyUsers (self, configurations, services, hosts):
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        users = {}

        if 'forced-configurations' not in services:
            services["forced-configurations"] = []

        if "HDFS" in servicesList:
            hdfs_user = None
            if "hadoop-env" in services["configurations"] and "hdfs_user" in services["configurations"]["hadoop-env"]["properties"]:
                hdfs_user = services["configurations"]["hadoop-env"]["properties"]["hdfs_user"]
                if not hdfs_user in users and hdfs_user is not None:
                    users[hdfs_user] = {"propertyHosts" : "*","propertyGroups" : "*", "config" : "hadoop-env", "propertyName" : "hdfs_user"}

        if "OOZIE" in servicesList:
            oozie_user = None
            if "oozie-env" in services["configurations"] and "oozie_user" in services["configurations"]["oozie-env"]["properties"]:
                oozie_user = services["configurations"]["oozie-env"]["properties"]["oozie_user"]
                oozieServerrHosts = self.getHostsWithComponent("OOZIE", "OOZIE_SERVER", services, hosts)
                if oozieServerrHosts is not None:
                    oozieServerHostsNameList = []
                    for oozieServerHost in oozieServerrHosts:
                        oozieServerHostsNameList.append(oozieServerHost["Hosts"]["host_name"])
                    oozieServerHostsNames = ",".join(oozieServerHostsNameList)
                    if not oozie_user in users and oozie_user is not None:
                        users[oozie_user] = {"propertyHosts" : oozieServerHostsNames,"propertyGroups" : "*", "config" : "oozie-env", "propertyName" : "oozie_user"}

        hive_user = None
        if "HIVE" in servicesList:
            webhcat_user = None
            if "hive-env" in services["configurations"] and "hive_user" in services["configurations"]["hive-env"]["properties"] \
                    and "webhcat_user" in services["configurations"]["hive-env"]["properties"]:
                hive_user = services["configurations"]["hive-env"]["properties"]["hive_user"]
                webhcat_user = services["configurations"]["hive-env"]["properties"]["webhcat_user"]
                hiveServerHosts = self.getHostsWithComponent("HIVE", "HIVE_SERVER", services, hosts)
                hiveServerInteractiveHosts = self.getHostsWithComponent("HIVE", "HIVE_SERVER_INTERACTIVE", services, hosts)
                webHcatServerHosts = self.getHostsWithComponent("HIVE", "WEBHCAT_SERVER", services, hosts)

                if hiveServerHosts is not None:
                    hiveServerHostsNameList = []
                    for hiveServerHost in hiveServerHosts:
                        hiveServerHostsNameList.append(hiveServerHost["Hosts"]["host_name"])
                    # Append Hive Server Interactive host as well, as it is Hive2/HiveServer2 component.
                    if hiveServerInteractiveHosts:
                        for hiveServerInteractiveHost in hiveServerInteractiveHosts:
                            hiveServerInteractiveHostName = hiveServerInteractiveHost["Hosts"]["host_name"]
                            if hiveServerInteractiveHostName not in hiveServerHostsNameList:
                                hiveServerHostsNameList.append(hiveServerInteractiveHostName)
                                Logger.info("Appended (if not exiting), Hive Server Interactive Host : '{0}', to Hive Server Host List : '{1}'".format(hiveServerInteractiveHostName, hiveServerHostsNameList))

                    hiveServerHostsNames = ",".join(hiveServerHostsNameList)  # includes Hive Server interactive host also.
                    Logger.info("Hive Server and Hive Server Interactive (if enabled) Host List : {0}".format(hiveServerHostsNameList))
                    if not hive_user in users and hive_user is not None:
                        users[hive_user] = {"propertyHosts" : hiveServerHostsNames,"propertyGroups" : "*", "config" : "hive-env", "propertyName" : "hive_user"}

                if webHcatServerHosts is not None:
                    webHcatServerHostsNameList = []
                    for webHcatServerHost in webHcatServerHosts:
                        webHcatServerHostsNameList.append(webHcatServerHost["Hosts"]["host_name"])
                    webHcatServerHostsNames = ",".join(webHcatServerHostsNameList)
                    if not webhcat_user in users and webhcat_user is not None:
                        users[webhcat_user] = {"propertyHosts" : webHcatServerHostsNames,"propertyGroups" : "*", "config" : "hive-env", "propertyName" : "webhcat_user"}

        if "YARN" in servicesList:
            yarn_user = None
            if "yarn-env" in services["configurations"] and "yarn_user" in services["configurations"]["yarn-env"]["properties"]:
                yarn_user = services["configurations"]["yarn-env"]["properties"]["yarn_user"]
                rmHosts = self.getHostsWithComponent("YARN", "RESOURCEMANAGER", services, hosts)

                if len(rmHosts) > 1:
                    rmHostsNameList = []
                    for rmHost in rmHosts:
                        rmHostsNameList.append(rmHost["Hosts"]["host_name"])
                    rmHostsNames = ",".join(rmHostsNameList)
                    if not yarn_user in users and yarn_user is not None:
                        users[yarn_user] = {"propertyHosts" : rmHostsNames, "config" : "yarn-env", "propertyName" : "yarn_user"}


        if "FALCON" in servicesList:
            falconUser = None
            if "falcon-env" in services["configurations"] and "falcon_user" in services["configurations"]["falcon-env"]["properties"]:
                falconUser = services["configurations"]["falcon-env"]["properties"]["falcon_user"]
                if not falconUser in users and falconUser is not None:
                    users[falconUser] = {"propertyHosts" : "*","propertyGroups" : "*", "config" : "falcon-env", "propertyName" : "falcon_user"}

        if "SPARK" in servicesList:
            livyUser = None
            if "livy-env" in services["configurations"] and "livy_user" in services["configurations"]["livy-env"]["properties"]:
                livyUser = services["configurations"]["livy-env"]["properties"]["livy_user"]
                if not livyUser in users and livyUser is not None:
                    users[livyUser] = {"propertyHosts" : "*","propertyGroups" : "*", "config" : "livy-env", "propertyName" : "livy_user"}

        putCoreSiteProperty = self.putProperty(configurations, "core-site", services)
        putCoreSitePropertyAttribute = self.putPropertyAttribute(configurations, "core-site")

        for user_name, user_properties in users.iteritems():
            if hive_user and hive_user == user_name:
                if "propertyHosts" in user_properties:
                    services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.hosts".format(hive_user)})
            # Add properties "hadoop.proxyuser.*.hosts", "hadoop.proxyuser.*.groups" to core-site for all users
            putCoreSiteProperty("hadoop.proxyuser.{0}.hosts".format(user_name) , user_properties["propertyHosts"])
            Logger.info("Updated hadoop.proxyuser.{0}.hosts as : {1}".format(hive_user, user_properties["propertyHosts"]))
            if "propertyGroups" in user_properties:
                putCoreSiteProperty("hadoop.proxyuser.{0}.groups".format(user_name) , user_properties["propertyGroups"])

            # Remove old properties if user was renamed
            userOldValue = self.getOldValue(services, user_properties["config"], user_properties["propertyName"])
            if userOldValue is not None and userOldValue != user_name:
                putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.hosts".format(userOldValue), 'delete', 'true')
                services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.hosts".format(userOldValue)})
                services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.hosts".format(user_name)})

                if "propertyGroups" in user_properties:
                    putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.groups".format(userOldValue), 'delete', 'true')
                    services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.groups".format(userOldValue)})
                    services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.groups".format(user_name)})

        self.recommendAmbariProxyUsersForHDFS(services, servicesList, putCoreSiteProperty, putCoreSitePropertyAttribute)

    def recommendHDFSConfigurations(self, configurations, clusterData, services, hosts):
        putHadoopEnvProperty = self.putProperty(configurations, "hadoop-env", services)
        putHdfsSiteProperty = self.putProperty(configurations, "hdfs-site", services)
        putHdfsSitePropertyAttributes = self.putPropertyAttribute(configurations, "hdfs-site")
        putCoreSitePropertyAttributes = self.putPropertyAttribute(configurations, "core-site")

        # HDP 2.0.6
        # calculate recommended memory
        putHadoopEnvProperty('namenode_heapsize', max(int(clusterData['totalAvailableRam'] / 2), 1024))
        putHadoopEnvProperty('namenode_opt_newsize', max(int(clusterData['totalAvailableRam'] / 8), 128))
        putHadoopEnvProperty('namenode_opt_maxnewsize', max(int(clusterData['totalAvailableRam'] / 8), 256))

        # recommended hdfs data location
        putHdfsSiteProperty("dfs.namenode.name.dir", "/var/hdfs/namenode")
        putHdfsSiteProperty("dfs.namenode.checkpoint.dir", "/var/hdfs/namesecondary")
        putHdfsSiteProperty("dfs.datanode.data.dir", "/var/hdfs/data")

        # Check if NN HA is enabled and recommend removing dfs.namenode.rpc-address
        hdfsSiteProperties = getServicesSiteProperties(services, "hdfs-site")
        nameServices = None
        if hdfsSiteProperties and 'dfs.internal.nameservices' in hdfsSiteProperties:
            nameServices = hdfsSiteProperties['dfs.internal.nameservices']
        if nameServices is None and hdfsSiteProperties and 'dfs.nameservices' in hdfsSiteProperties:
            nameServices = hdfsSiteProperties['dfs.nameservices']
        if nameServices and "dfs.ha.namenodes.%s" % nameServices in hdfsSiteProperties:
            namenodes = hdfsSiteProperties["dfs.ha.namenodes.%s" % nameServices]
            if len(namenodes.split(',')) > 1:
                putHdfsSitePropertyAttributes("dfs.namenode.rpc-address", "delete", "true")

        #Initialize default 'dfs.datanode.data.dir' if needed
        if (not hdfsSiteProperties) or ('dfs.datanode.data.dir' not in hdfsSiteProperties):
            dataDirs = '/hadoop/hdfs/data'
            putHdfsSiteProperty('dfs.datanode.data.dir', dataDirs)
        else:
            dataDirs = hdfsSiteProperties['dfs.datanode.data.dir'].split(",")

        # dfs.datanode.du.reserved should be set to 10-15% of volume size
        # For each host selects maximum size of the volume. Then gets minimum for all hosts.
        # This ensures that each host will have at least one data dir with available space.
        reservedSizeRecommendation = 0l #kBytes
        for host in hosts["items"]:
            mountPoints = []
            mountPointDiskAvailableSpace = [] #kBytes
            for diskInfo in host["Hosts"]["disk_info"]:
                mountPoints.append(diskInfo["mountpoint"])
                mountPointDiskAvailableSpace.append(long(diskInfo["size"]))

            maxFreeVolumeSizeForHost = 0l #kBytes
            for dataDir in dataDirs:
                mp = getMountPointForDir(dataDir, mountPoints)
                for i in range(len(mountPoints)):
                    if mp == mountPoints[i]:
                        if mountPointDiskAvailableSpace[i] > maxFreeVolumeSizeForHost:
                            maxFreeVolumeSizeForHost = mountPointDiskAvailableSpace[i]

            if not reservedSizeRecommendation or maxFreeVolumeSizeForHost and maxFreeVolumeSizeForHost < reservedSizeRecommendation:
                reservedSizeRecommendation = maxFreeVolumeSizeForHost

        if reservedSizeRecommendation:
            reservedSizeRecommendation = max(reservedSizeRecommendation * 1024 / 8, 1073741824) # At least 1Gb is reserved
            putHdfsSiteProperty('dfs.datanode.du.reserved', reservedSizeRecommendation) #Bytes

        # recommendations for "hadoop.proxyuser.*.hosts", "hadoop.proxyuser.*.groups" properties in core-site
        self.recommendHadoopProxyUsers(configurations, services, hosts)
        # end of HDP 2.0.6

        # HDP 2.2 stack
        putHdfsSiteProperty("dfs.datanode.max.transfer.threads", 16384 if clusterData["hBaseInstalled"] else 4096)
        dataDirsCount = 1
        # Use users 'dfs.datanode.data.dir' first
        if "hdfs-site" in services["configurations"] and "dfs.datanode.data.dir" in services["configurations"]["hdfs-site"]["properties"]:
            dataDirsCount = len(str(services["configurations"]["hdfs-site"]["properties"]["dfs.datanode.data.dir"]).split(","))
        elif "dfs.datanode.data.dir" in configurations["hdfs-site"]["properties"]:
            dataDirsCount = len(str(configurations["hdfs-site"]["properties"]["dfs.datanode.data.dir"]).split(","))
        if dataDirsCount <= 2:
            failedVolumesTolerated = 0
        elif dataDirsCount <= 4:
            failedVolumesTolerated = 1
        else:
            failedVolumesTolerated = 2
        putHdfsSiteProperty("dfs.datanode.failed.volumes.tolerated", failedVolumesTolerated)

        namenodeHosts = self.getHostsWithComponent("HDFS", "NAMENODE", services, hosts)

        # 25 * # of cores on NameNode
        nameNodeCores = 4
        if namenodeHosts is not None and len(namenodeHosts):
            nameNodeCores = int(namenodeHosts[0]['Hosts']['cpu_count'])
        putHdfsSiteProperty("dfs.namenode.handler.count", 25 * nameNodeCores)
        if 25 * nameNodeCores > 200:
            putHdfsSitePropertyAttributes("dfs.namenode.handler.count", "maximum", 25 * nameNodeCores)

        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        if ('ranger-hdfs-plugin-properties' in services['configurations']) and ('ranger-hdfs-plugin-enabled' in services['configurations']['ranger-hdfs-plugin-properties']['properties']):
            rangerPluginEnabled = services['configurations']['ranger-hdfs-plugin-properties']['properties']['ranger-hdfs-plugin-enabled']
            if ("RANGER" in servicesList) and (rangerPluginEnabled.lower() == 'Yes'.lower()):
                putHdfsSiteProperty("dfs.permissions.enabled",'true')

        putHdfsSiteProperty("dfs.namenode.safemode.threshold-pct", "0.999" if len(namenodeHosts) > 1 else "1.000")

        putHdfsEnvProperty = self.putProperty(configurations, "hadoop-env", services)
        putHdfsEnvPropertyAttribute = self.putPropertyAttribute(configurations, "hadoop-env")

        putHdfsEnvProperty('namenode_heapsize', max(int(clusterData['totalAvailableRam'] / 2), 1024))

        nn_heapsize_limit = None
        if (namenodeHosts is not None and len(namenodeHosts) > 0):
            if len(namenodeHosts) > 1:
                nn_max_heapsize = min(int(namenodeHosts[0]["Hosts"]["total_mem"]), int(namenodeHosts[1]["Hosts"]["total_mem"])) / 1024
                masters_at_host = max(self.getHostComponentsByCategories(namenodeHosts[0]["Hosts"]["host_name"], ["MASTER"], services, hosts),
                                      self.getHostComponentsByCategories(namenodeHosts[1]["Hosts"]["host_name"], ["MASTER"], services,hosts))
            else:
                nn_max_heapsize = int(namenodeHosts[0]["Hosts"]["total_mem"] / 1024)  # total_mem in kb
                masters_at_host = self.getHostComponentsByCategories(namenodeHosts[0]["Hosts"]["host_name"], ["MASTER"], services, hosts)

            putHdfsEnvPropertyAttribute('namenode_heapsize', 'maximum', max(nn_max_heapsize, 1024))

            nn_heapsize_limit = nn_max_heapsize
            nn_heapsize_limit -= clusterData["reservedRam"]
            if len(masters_at_host) > 1:
                nn_heapsize_limit = int(nn_heapsize_limit / 2)

            putHdfsEnvProperty('namenode_heapsize', max(nn_heapsize_limit, 1024))

        datanodeHosts = self.getHostsWithComponent("HDFS", "DATANODE", services, hosts)
        if datanodeHosts is not None and len(datanodeHosts) > 0:
            min_datanode_ram_kb = 1073741824  # 1 TB
            for datanode in datanodeHosts:
                ram_kb = datanode['Hosts']['total_mem']
                min_datanode_ram_kb = min(min_datanode_ram_kb, ram_kb)

            datanodeFilesM = len(datanodeHosts) * dataDirsCount / 10  # in millions, # of files = # of disks * 100'000
            nn_memory_configs = [
                {'nn_heap': 1024, 'nn_opt': 128},
                {'nn_heap': 3072, 'nn_opt': 512},
                {'nn_heap': 5376, 'nn_opt': 768},
                {'nn_heap': 9984, 'nn_opt': 1280},
                {'nn_heap': 14848, 'nn_opt': 2048},
                {'nn_heap': 19456, 'nn_opt': 2560},
                {'nn_heap': 24320, 'nn_opt': 3072},
                {'nn_heap': 33536, 'nn_opt': 4352},
                {'nn_heap': 47872, 'nn_opt': 6144},
                {'nn_heap': 59648, 'nn_opt': 7680},
                {'nn_heap': 71424, 'nn_opt': 8960},
                {'nn_heap': 94976, 'nn_opt': 8960}
            ]
            index = {
                datanodeFilesM < 1: 0,
                1 <= datanodeFilesM < 5: 1,
                5 <= datanodeFilesM < 10: 2,
                10 <= datanodeFilesM < 20: 3,
                20 <= datanodeFilesM < 30: 4,
                30 <= datanodeFilesM < 40: 5,
                40 <= datanodeFilesM < 50: 6,
                50 <= datanodeFilesM < 70: 7,
                70 <= datanodeFilesM < 100: 8,
                100 <= datanodeFilesM < 125: 9,
                125 <= datanodeFilesM < 150: 10,
                150 <= datanodeFilesM: 11
            }[1]

            nn_memory_config = nn_memory_configs[index]

            # override with new values if applicable
            if nn_heapsize_limit is not None and nn_memory_config['nn_heap'] <= nn_heapsize_limit:
                putHdfsEnvProperty('namenode_heapsize', nn_memory_config['nn_heap'])

            putHdfsEnvPropertyAttribute('dtnode_heapsize', 'maximum', int(min_datanode_ram_kb / 1024))

        nn_heapsize = int(configurations["hadoop-env"]["properties"]["namenode_heapsize"])
        putHdfsEnvProperty('namenode_opt_newsize', max(int(nn_heapsize / 8), 128))
        putHdfsEnvProperty('namenode_opt_maxnewsize', max(int(nn_heapsize / 8), 128))

        putHdfsSitePropertyAttribute = self.putPropertyAttribute(configurations, "hdfs-site")
        putHdfsSitePropertyAttribute('dfs.datanode.failed.volumes.tolerated', 'maximum', dataDirsCount)

        keyserverHostsString = None
        keyserverPortString = None
        if "hadoop-env" in services["configurations"] and "keyserver_host" in \
            services["configurations"]["hadoop-env"]["properties"] and "keyserver_port" in \
            services["configurations"]["hadoop-env"]["properties"]:
            keyserverHostsString = services["configurations"]["hadoop-env"]["properties"]["keyserver_host"]
            keyserverPortString = services["configurations"]["hadoop-env"]["properties"]["keyserver_port"]

        # Irrespective of what hadoop-env has, if Ranger-KMS is installed, we use its values.
        rangerKMSServerHosts = self.getHostsWithComponent("RANGER_KMS", "RANGER_KMS_SERVER", services, hosts)
        if rangerKMSServerHosts is not None and len(rangerKMSServerHosts) > 0:
            rangerKMSServerHostsArray = []
            for rangeKMSServerHost in rangerKMSServerHosts:
                rangerKMSServerHostsArray.append(rangeKMSServerHost["Hosts"]["host_name"])
            keyserverHostsString = ";".join(rangerKMSServerHostsArray)
            if "kms-env" in services["configurations"] and "kms_port" in services["configurations"]["kms-env"]["properties"]:
                keyserverPortString = services["configurations"]["kms-env"]["properties"]["kms_port"]

        if keyserverHostsString is not None and len(keyserverHostsString.strip()) > 0:
            urlScheme = "http"
            if "ranger-kms-site" in services["configurations"] and "ranger.service.https.attrib.ssl.enabled" in services["configurations"]["ranger-kms-site"]["properties"] and \
                            services["configurations"]["ranger-kms-site"]["properties"]["ranger.service.https.attrib.ssl.enabled"].lower() == "true":
                urlScheme = "https"

            if keyserverPortString is None or len(keyserverPortString.strip()) < 1:
                keyserverPortString = ":9292"
            else:
                keyserverPortString = ":" + keyserverPortString.strip()
            putCoreSiteProperty = self.putProperty(configurations, "core-site", services)
            kmsPath = "kms://" + urlScheme + "@" + keyserverHostsString.strip() + keyserverPortString + "/kms"
            putCoreSiteProperty("hadoop.security.key.provider.path", kmsPath)
            putHdfsSiteProperty("dfs.encryption.key.provider.uri", kmsPath)

        if "ranger-env" in services["configurations"] and "ranger-hdfs-plugin-properties" in services["configurations"] and \
            "ranger-hdfs-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            putHdfsRangerPluginProperty = self.putProperty(configurations, "ranger-hdfs-plugin-properties", services)
            rangerEnvHdfsPluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-hdfs-plugin-enabled"]
            putHdfsRangerPluginProperty("ranger-hdfs-plugin-enabled", rangerEnvHdfsPluginProperty)

        if ('ranger-hdfs-plugin-properties' in services['configurations']) and ('ranger-hdfs-plugin-enabled' in services['configurations']['ranger-hdfs-plugin-properties']['properties']):
            rangerPluginEnabled = ''
            if 'ranger-hdfs-plugin-properties' in configurations and 'ranger-hdfs-plugin-enabled' in configurations['ranger-hdfs-plugin-properties']['properties']:
                rangerPluginEnabled = configurations['ranger-hdfs-plugin-properties']['properties']['ranger-hdfs-plugin-enabled']
            elif 'ranger-hdfs-plugin-properties' in services['configurations'] and 'ranger-hdfs-plugin-enabled' in services['configurations']['ranger-hdfs-plugin-properties']['properties']:
                rangerPluginEnabled = services['configurations']['ranger-hdfs-plugin-properties']['properties']['ranger-hdfs-plugin-enabled']

            if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
                putHdfsSiteProperty("dfs.namenode.inode.attributes.provider.class", 'org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer')
            else:
                putHdfsSitePropertyAttributes('dfs.namenode.inode.attributes.provider.class', 'delete', 'true')
        else:
            putHdfsSitePropertyAttributes('dfs.namenode.inode.attributes.provider.class', 'delete', 'true')

        if not "RANGER_KMS" in servicesList:
            putCoreSitePropertyAttributes('hadoop.security.key.provider.path', 'delete', 'true')
            putHdfsSitePropertyAttributes('dfs.encryption.key.provider.uri', 'delete', 'true')
        # end of HDP 2.2 stack



    def recommendHbaseConfigurations(self, configurations, clusterData, services, hosts):
        # recommendations for HBase env config

        # If cluster size is < 100, hbase master heap = 2G
        # else If cluster size is < 500, hbase master heap = 4G
        # else hbase master heap = 8G
        # for small test clusters use 1 gb
        hostsCount = 0
        if hosts and "items" in hosts:
            hostsCount = len(hosts["items"])

        hbaseMasterRam = {
            hostsCount < 20: 1,
            20 <= hostsCount < 100: 2,
            100 <= hostsCount < 500: 4,
            500 <= hostsCount: 8
        }[True]

        putHbaseProperty = self.putProperty(configurations, "hbase-env", services)
        putHbaseProperty('hbase_regionserver_heapsize', int(clusterData['hbaseRam']) * 1024)
        putHbaseProperty('hbase_master_heapsize', hbaseMasterRam * 1024)

        # recommendations for HBase site config
        putHbaseSiteProperty = self.putProperty(configurations, "hbase-site", services)

        if 'hbase-site' in services['configurations'] and 'hbase.superuser' in services['configurations']['hbase-site']['properties'] \
                and 'hbase-env' in services['configurations'] and 'hbase_user' in services['configurations']['hbase-env']['properties'] \
                and services['configurations']['hbase-env']['properties']['hbase_user'] != services['configurations']['hbase-site']['properties']['hbase.superuser']:
            putHbaseSiteProperty("hbase.superuser", services['configurations']['hbase-env']['properties']['hbase_user'])

        if "ranger-env" in services["configurations"] and "ranger-hbase-plugin-properties" in services["configurations"] and \
                        "ranger-hbase-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            putHbaseRangerPluginProperty = self.putProperty(configurations, "ranger-hbase-plugin-properties", services)
            rangerEnvHbasePluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-hbase-plugin-enabled"]
            putHbaseRangerPluginProperty("ranger-hbase-plugin-enabled", rangerEnvHbasePluginProperty)
            if "cluster-env" in services["configurations"] and "smokeuser" in services["configurations"]["cluster-env"][
                "properties"]:
                smoke_user = services["configurations"]["cluster-env"]["properties"]["smokeuser"]
                putHbaseRangerPluginProperty("policy_user", smoke_user)
        rangerPluginEnabled = ''
        if 'ranger-hbase-plugin-properties' in configurations and 'ranger-hbase-plugin-enabled' in \
                configurations['ranger-hbase-plugin-properties']['properties']:
            rangerPluginEnabled = configurations['ranger-hbase-plugin-properties']['properties']['ranger-hbase-plugin-enabled']
        elif 'ranger-hbase-plugin-properties' in services['configurations'] and 'ranger-hbase-plugin-enabled' in \
                services['configurations']['ranger-hbase-plugin-properties']['properties']:
            rangerPluginEnabled = services['configurations']['ranger-hbase-plugin-properties']['properties']['ranger-hbase-plugin-enabled']

        if rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
            putHbaseSiteProperty('hbase.security.authorization', 'true')

        # Authorization
        hbaseCoProcessorConfigs = {
            'hbase.coprocessor.region.classes': [],
            'hbase.coprocessor.regionserver.classes': [],
            'hbase.coprocessor.master.classes': []
        }

        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        rangerServiceVersion = ''
        if 'RANGER' in servicesList:
            rangerServiceVersion = [service['StackServices']['service_version'] for service in services["services"] if
                                    service['StackServices']['service_name'] == 'RANGER'][0]

        rangerClass = 'org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor'

        nonRangerClass = 'org.apache.hadoop.hbase.security.access.AccessController'
        hbaseClassConfigs = hbaseCoProcessorConfigs.keys()

        for item in range(len(hbaseClassConfigs)):
            if 'hbase-site' in services['configurations']:
                if hbaseClassConfigs[item] in services['configurations']['hbase-site']['properties']:
                    if 'hbase-site' in configurations and hbaseClassConfigs[item] in configurations['hbase-site'][
                        'properties']:
                        coprocessorConfig = configurations['hbase-site']['properties'][hbaseClassConfigs[item]]
                    else:
                        coprocessorConfig = services['configurations']['hbase-site']['properties'][hbaseClassConfigs[item]]
                    coprocessorClasses = coprocessorConfig.split(",")
                    coprocessorClasses = filter(None, coprocessorClasses)  # Removes empty string elements from array
                    if rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
                        if nonRangerClass in coprocessorClasses:
                            coprocessorClasses.remove(nonRangerClass)
                        if not rangerClass in coprocessorClasses:
                            coprocessorClasses.append(rangerClass)
                        putHbaseSiteProperty(hbaseClassConfigs[item], ','.join(coprocessorClasses))
                    elif rangerPluginEnabled and rangerPluginEnabled.lower() == 'No'.lower():
                        if rangerClass in coprocessorClasses:
                            coprocessorClasses.remove(rangerClass)
                            if not nonRangerClass in coprocessorClasses:
                                coprocessorClasses.append(nonRangerClass)
                            putHbaseSiteProperty(hbaseClassConfigs[item], ','.join(coprocessorClasses))
                elif rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
                    putHbaseSiteProperty(hbaseClassConfigs[item], rangerClass)

    def recommendRangerConfigurations(self, configurations, clusterData, services, hosts):

        putRangerAdminProperty = self.putProperty(configurations, "admin-properties", services)
        putRangerEnvProperty = self.putProperty(configurations, "ranger-env", services)

        # Build policymgr_external_url
        protocol = 'http'
        ranger_admin_host = 'localhost'
        port = '6080'

        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

        # Check if http is disabled. For HDP-2.3 this can be checked in ranger-admin-site/ranger.service.http.enabled
        # For Ranger-0.4.0 this can be checked in ranger-site/http.enabled
        if ('ranger-site' in services['configurations'] and 'http.enabled' in services['configurations']['ranger-site']['properties'] \
                    and services['configurations']['ranger-site']['properties']['http.enabled'].lower() == 'false') or \
                ('ranger-admin-site' in services['configurations'] and 'ranger.service.http.enabled' in services['configurations']['ranger-admin-site']['properties'] \
                         and services['configurations']['ranger-admin-site']['properties']['ranger.service.http.enabled'].lower() == 'false'):
            # HTTPS protocol is used
            protocol = 'https'
            # Starting Ranger-0.5.0.2.3 port stored in ranger-admin-site ranger.service.https.port
            if 'ranger-admin-site' in services['configurations'] and 'ranger.service.https.port' in services['configurations']['ranger-admin-site']['properties']:
                port = services['configurations']['ranger-admin-site']['properties']['ranger.service.https.port']
            # In Ranger-0.4.0 port stored in ranger-site https.service.port
            elif 'ranger-site' in services['configurations'] and 'https.service.port' in services['configurations']['ranger-site']['properties']:
                port = services['configurations']['ranger-site']['properties']['https.service.port']
        else:
            # HTTP protocol is used
            # Starting Ranger-0.5.0.2.3 port stored in ranger-admin-site ranger.service.http.port
            if 'ranger-admin-site' in services['configurations'] and 'ranger.service.http.port' in services['configurations']['ranger-admin-site']['properties']:
                port = services['configurations']['ranger-admin-site']['properties']['ranger.service.http.port']
            # In Ranger-0.4.0 port stored in ranger-site http.service.port
            elif 'ranger-site' in services['configurations'] and 'http.service.port' in services['configurations']['ranger-site']['properties']:
                port = services['configurations']['ranger-site']['properties']['http.service.port']

        ranger_admin_hosts = self.getComponentHostNames(services, "RANGER", "RANGER_ADMIN")
        if ranger_admin_hosts:
            if len(ranger_admin_hosts) > 1 \
                    and services['configurations'] \
                    and 'admin-properties' in services['configurations'] and 'policymgr_external_url' in services['configurations']['admin-properties']['properties'] \
                    and services['configurations']['admin-properties']['properties']['policymgr_external_url'] \
                    and services['configurations']['admin-properties']['properties']['policymgr_external_url'].strip():

                # in case of HA deployment keep the policymgr_external_url specified in the config
                policymgr_external_url = services['configurations']['admin-properties']['properties']['policymgr_external_url']
            else:

                ranger_admin_host = ranger_admin_hosts[0]
                policymgr_external_url = "%s://%s:%s" % (protocol, ranger_admin_host, port)

            putRangerAdminProperty('policymgr_external_url', policymgr_external_url)

        ranger_plugins_serviceuser = [
            {'service_name': 'HDFS', 'file_name': 'hadoop-env', 'config_name': 'hdfs_user', 'target_configname': 'ranger.plugins.hdfs.serviceuser'},
            {'service_name': 'HIVE', 'file_name': 'hive-env', 'config_name': 'hive_user', 'target_configname': 'ranger.plugins.hive.serviceuser'},
            {'service_name': 'YARN', 'file_name': 'yarn-env', 'config_name': 'yarn_user', 'target_configname': 'ranger.plugins.yarn.serviceuser'},
            {'service_name': 'HBASE', 'file_name': 'hbase-env', 'config_name': 'hbase_user', 'target_configname': 'ranger.plugins.hbase.serviceuser'},
            {'service_name': 'KNOX', 'file_name': 'knox-env', 'config_name': 'knox_user', 'target_configname': 'ranger.plugins.knox.serviceuser'},
            {'service_name': 'STORM', 'file_name': 'storm-env', 'config_name': 'storm_user', 'target_configname': 'ranger.plugins.storm.serviceuser'},
            {'service_name': 'KAFKA', 'file_name': 'kafka-env', 'config_name': 'kafka_user', 'target_configname': 'ranger.plugins.kafka.serviceuser'},
            {'service_name': 'RANGER_KMS', 'file_name': 'kms-env', 'config_name': 'kms_user', 'target_configname': 'ranger.plugins.kms.serviceuser'},
            {'service_name': 'ATLAS', 'file_name': 'atlas-env', 'config_name': 'metadata_user', 'target_configname': 'ranger.plugins.atlas.serviceuser'}
        ]

        for item in range(len(ranger_plugins_serviceuser)):
            if ranger_plugins_serviceuser[item]['service_name'] in servicesList:
                file_name = ranger_plugins_serviceuser[item]['file_name']
                config_name = ranger_plugins_serviceuser[item]['config_name']
                target_configname = ranger_plugins_serviceuser[item]['target_configname']
                if file_name in services["configurations"] and config_name in services["configurations"][file_name]["properties"]:
                    service_user = services["configurations"][file_name]["properties"][config_name]
                    putRangerAdminProperty(target_configname, service_user)

        # Recommend ranger.audit.solr.zookeepers and xasecure.audit.destination.hdfs.dir
        include_hdfs = "HDFS" in servicesList
        if include_hdfs:
            if 'core-site' in services['configurations'] and ('fs.defaultFS' in services['configurations']['core-site']['properties']):
                default_fs = services['configurations']['core-site']['properties']['fs.defaultFS']
                putRangerEnvProperty('xasecure.audit.destination.hdfs.dir', '{0}/{1}/{2}'.format(default_fs, 'ranger', 'audit'))

        # Recommend Ranger supported service's audit properties
        ranger_services = [
            {'service_name': 'HDFS', 'audit_file': 'ranger-hdfs-audit'},
            {'service_name': 'YARN', 'audit_file': 'ranger-yarn-audit'},
            {'service_name': 'HBASE', 'audit_file': 'ranger-hbase-audit'},
            {'service_name': 'HIVE', 'audit_file': 'ranger-hive-audit'},
            {'service_name': 'KNOX', 'audit_file': 'ranger-knox-audit'},
            {'service_name': 'KAFKA', 'audit_file': 'ranger-kafka-audit'},
            {'service_name': 'RANGER_KMS', 'audit_file': 'ranger-kms-audit'},
            {'service_name': 'STORM', 'audit_file': 'ranger-storm-audit'}
        ]

        for item in range(len(ranger_services)):
            if ranger_services[item]['service_name'] in servicesList:
                component_audit_file = ranger_services[item]['audit_file']
                if component_audit_file in services["configurations"]:
                    ranger_audit_dict = [
                        {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.db', 'target_configname': 'xasecure.audit.destination.db'},
                        {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.hdfs', 'target_configname': 'xasecure.audit.destination.hdfs'},
                        {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.hdfs.dir', 'target_configname': 'xasecure.audit.destination.hdfs.dir'},
                        {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.solr', 'target_configname': 'xasecure.audit.destination.solr'},
                        {'filename': 'ranger-admin-site', 'configname': 'ranger.audit.solr.urls', 'target_configname': 'xasecure.audit.destination.solr.urls'},
                        {'filename': 'ranger-admin-site', 'configname': 'ranger.audit.solr.zookeepers', 'target_configname': 'xasecure.audit.destination.solr.zookeepers'}
                    ]
                    putRangerAuditProperty = self.putProperty(configurations, component_audit_file, services)

                    for item in ranger_audit_dict:
                        if item['filename'] in services["configurations"] and item['configname'] in \
                                services["configurations"][item['filename']]["properties"]:
                            if item['filename'] in configurations and item['configname'] in configurations[item['filename']]["properties"]:
                                rangerAuditProperty = configurations[item['filename']]["properties"][item['configname']]
                            else:
                                rangerAuditProperty = services["configurations"][item['filename']]["properties"][item['configname']]
                            putRangerAuditProperty(item['target_configname'], rangerAuditProperty)

        if "HDFS" in servicesList:
            hdfs_user = None
            if "hadoop-env" in services["configurations"] and "hdfs_user" in services["configurations"]["hadoop-env"]["properties"]:
                hdfs_user = services["configurations"]["hadoop-env"]["properties"]["hdfs_user"]
                putRangerAdminProperty('ranger.kms.service.user.hdfs', hdfs_user)

        if "HIVE" in servicesList:
            hive_user = None
            if "hive-env" in services["configurations"] and "hive_user" in services["configurations"]["hive-env"]["properties"]:
                hive_user = services["configurations"]["hive-env"]["properties"]["hive_user"]
                putRangerAdminProperty('ranger.kms.service.user.hive', hive_user)

        for item in range(len(ranger_plugins_serviceuser)):
            if ranger_plugins_serviceuser[item]['service_name'] in servicesList:
                file_name = ranger_plugins_serviceuser[item]['file_name']
                config_name = ranger_plugins_serviceuser[item]['config_name']
                target_configname = ranger_plugins_serviceuser[item]['target_configname']
                if file_name in services["configurations"] and config_name in services["configurations"][file_name]["properties"]:
                    service_user = services["configurations"][file_name]["properties"][config_name]
                    putRangerAdminProperty(target_configname, service_user)

    def recommendRangerKMSConfigurations(self, configurations, clusterData, services, hosts):
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        putRangerKmsDbksProperty = self.putProperty(configurations, "dbks-site", services)
        putRangerKmsProperty = self.putProperty(configurations, "kms-properties", services)
        putRangerKmsSiteProperty = self.putProperty(configurations, "kms-site", services)
        kmsEnvProperties = getSiteProperties(services['configurations'], 'kms-env')
        putCoreSiteProperty = self.putProperty(configurations, "core-site", services)
        putCoreSitePropertyAttribute = self.putPropertyAttribute(configurations, "core-site")
        putRangerKmsAuditProperty = self.putProperty(configurations, "ranger-kms-audit", services)

        if 'kms-properties' in services['configurations'] and (
            'DB_FLAVOR' in services['configurations']['kms-properties']['properties']):

            rangerKmsDbFlavor = services['configurations']["kms-properties"]["properties"]["DB_FLAVOR"]

            if ('db_host' in services['configurations']['kms-properties']['properties']) and (
                'db_name' in services['configurations']['kms-properties']['properties']):

                rangerKmsDbHost = services['configurations']["kms-properties"]["properties"]["db_host"]
                rangerKmsDbName = services['configurations']["kms-properties"]["properties"]["db_name"]

                ranger_kms_db_url_dict = {
                    'MYSQL': {'ranger.ks.jpa.jdbc.driver': 'com.mysql.jdbc.Driver',
                              'ranger.ks.jpa.jdbc.url': 'jdbc:mysql://' + self.getDBConnectionHostPort(rangerKmsDbFlavor, rangerKmsDbHost) + '/' + rangerKmsDbName},
                    'ORACLE': {'ranger.ks.jpa.jdbc.driver': 'oracle.jdbc.driver.OracleDriver',
                               'ranger.ks.jpa.jdbc.url': 'jdbc:oracle:thin:@' + self.getOracleDBConnectionHostPort(rangerKmsDbFlavor, rangerKmsDbHost, rangerKmsDbName)},
                    'POSTGRES': {'ranger.ks.jpa.jdbc.driver': 'org.postgresql.Driver',
                                 'ranger.ks.jpa.jdbc.url': 'jdbc:postgresql://' + self.getDBConnectionHostPort(rangerKmsDbFlavor, rangerKmsDbHost) + '/' + rangerKmsDbName},
                    'MSSQL': {'ranger.ks.jpa.jdbc.driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                              'ranger.ks.jpa.jdbc.url': 'jdbc:sqlserver://' + self.getDBConnectionHostPort(rangerKmsDbFlavor, rangerKmsDbHost) + ';databaseName=' + rangerKmsDbName},
                    'SQLA': {'ranger.ks.jpa.jdbc.driver': 'sap.jdbc4.sqlanywhere.IDriver',
                             'ranger.ks.jpa.jdbc.url': 'jdbc:sqlanywhere:host=' + self.getDBConnectionHostPort(rangerKmsDbFlavor, rangerKmsDbHost) + ';database=' + rangerKmsDbName}
                }

                rangerKmsDbProperties = ranger_kms_db_url_dict.get(rangerKmsDbFlavor, ranger_kms_db_url_dict['MYSQL'])
                for key in rangerKmsDbProperties:
                    putRangerKmsDbksProperty(key, rangerKmsDbProperties.get(key))

        if kmsEnvProperties and self.checkSiteProperties(kmsEnvProperties, 'kms_user') and 'KERBEROS' in servicesList:
            kmsUser = kmsEnvProperties['kms_user']
            kmsUserOld = self.getOldValue(services, 'kms-env', 'kms_user')
            putCoreSiteProperty('hadoop.proxyuser.{0}.groups'.format(kmsUser), '*')
            if kmsUserOld is not None and kmsUser != kmsUserOld:
                putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.groups".format(kmsUserOld), 'delete', 'true')
                services["forced-configurations"].append({"type": "core-site", "name": "hadoop.proxyuser.{0}.groups".format(kmsUserOld)})
                services["forced-configurations"].append({"type": "core-site", "name": "hadoop.proxyuser.{0}.groups".format(kmsUser)})

        if "HDFS" in servicesList:
            if 'core-site' in services['configurations'] and (
                'fs.defaultFS' in services['configurations']['core-site']['properties']):
                default_fs = services['configurations']['core-site']['properties']['fs.defaultFS']
                putRangerKmsAuditProperty('xasecure.audit.destination.hdfs.dir',
                                          '{0}/{1}/{2}'.format(default_fs, 'ranger', 'audit'))

        #from stack 2.5
        if 'ranger-env' in services['configurations'] and 'ranger_user' in services['configurations']['ranger-env']['properties']:
            rangerUser = services['configurations']['ranger-env']['properties']['ranger_user']

            if 'kms-site' in services['configurations'] and 'KERBEROS' in servicesList:
                putRangerKmsSiteProperty('hadoop.kms.proxyuser.{0}.groups'.format(rangerUser), '*')
                putRangerKmsSiteProperty('hadoop.kms.proxyuser.{0}.hosts'.format(rangerUser), '*')
                putRangerKmsSiteProperty('hadoop.kms.proxyuser.{0}.users'.format(rangerUser), '*')

    def getOracleDBConnectionHostPort(self, db_type, db_host, rangerDbName):
        connection_string = self.getDBConnectionHostPort(db_type, db_host)
        colon_count = db_host.count(':')
        if colon_count == 1 and '/' in db_host:
            connection_string = "//" + connection_string
        elif colon_count == 0 or colon_count == 1:
            connection_string = "//" + connection_string + "/" + rangerDbName if rangerDbName else "//" + connection_string

        return connection_string

    def getDBConnectionHostPort(self, db_type, db_host):
        DB_TYPE_DEFAULT_PORT_MAP = {"MYSQL": "3306", "ORACLE": "1521", "POSTGRES": "5432", "MSSQL": "1433", "SQLA": "2638"}
        connection_string = ""
        if db_type is None or db_type == "":
            return connection_string
        else:
            colon_count = db_host.count(':')
            if colon_count == 0:
                if DB_TYPE_DEFAULT_PORT_MAP.has_key(db_type):
                    connection_string = db_host + ":" + DB_TYPE_DEFAULT_PORT_MAP[db_type]
                else:
                    connection_string = db_host
            elif colon_count == 1:
                connection_string = db_host
            elif colon_count == 2:
                connection_string = db_host

        return connection_string

    def recommendHiveConfigurations(self, configurations, clusterData, services, hosts):
        hiveSiteProperties = getSiteProperties(services['configurations'], 'hive-site')
        hiveEnvProperties = getSiteProperties(services['configurations'], 'hive-env')
        containerSize = clusterData['mapMemory'] if clusterData['mapMemory'] > 2048 else int(clusterData['reduceMemory'])
        containerSize = min(clusterData['containers'] * clusterData['ramPerContainer'], containerSize)
        container_size_bytes = int(containerSize)*1024*1024

        putHiveEnvProperty = self.putProperty(configurations, "hive-env", services)
        putHiveSiteProperty = self.putProperty(configurations, "hive-site", services)
        putHiveServerProperty = self.putProperty(configurations, "hiveserver2-site", services)
        putHiveServerPropertyAttribute = self.putPropertyAttribute(configurations, "hiveserver2-site")

        putHiveSiteProperty('hive.auto.convert.join.noconditionaltask.size', int(round(container_size_bytes / 3)))
        putHiveSiteProperty('hive.tez.java.opts', "-server -Xmx" + str(int(round((0.8 * containerSize) + 0.5)))
                        + "m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseParallelGC -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps")
        putHiveSiteProperty('hive.tez.container.size', containerSize)

        # javax.jdo.option.ConnectionURL recommendations
        if hiveEnvProperties and self.checkSiteProperties(hiveEnvProperties, 'hive_database', 'hive_database_type'):
            putHiveEnvProperty('hive_database_type', self.getDBTypeAlias(hiveEnvProperties['hive_database']))
        if hiveEnvProperties and hiveSiteProperties and self.checkSiteProperties(hiveSiteProperties, 'javax.jdo.option.ConnectionDriverName') and self.checkSiteProperties(hiveEnvProperties, 'hive_database'):
            putHiveSiteProperty('javax.jdo.option.ConnectionDriverName', self.getDBDriver(hiveEnvProperties['hive_database']))
        if hiveSiteProperties and hiveEnvProperties and self.checkSiteProperties(hiveSiteProperties, 'ambari.hive.db.schema.name', 'javax.jdo.option.ConnectionURL') and self.checkSiteProperties(hiveEnvProperties, 'hive_database'):
            hiveServerHost = self.getHostWithComponent('HIVE', 'HIVE_SERVER', services, hosts)
            hiveDBConnectionURL = hiveSiteProperties['javax.jdo.option.ConnectionURL']
            protocol = self.getProtocol(hiveEnvProperties['hive_database'])
            oldSchemaName = self.getOldValue(services, "hive-site", "ambari.hive.db.schema.name")
            oldDBType = self.getOldValue(services, "hive-env", "hive_database")
            # under these if constructions we are checking if hive server hostname available,
            # if it's default db connection url with "localhost" or if schema name was changed or if db type was changed (only for db type change from default mysql to existing mysql)
            # or if protocol according to current db type differs with protocol in db connection url(other db types changes)
            if hiveServerHost is not None:
                if (hiveDBConnectionURL and "//localhost" in hiveDBConnectionURL) or oldSchemaName or oldDBType or (protocol and hiveDBConnectionURL and not hiveDBConnectionURL.startswith(protocol)):
                    dbConnection = self.getDBConnectionString(hiveEnvProperties['hive_database']).format(hiveServerHost['Hosts']['host_name'], hiveSiteProperties['ambari.hive.db.schema.name'])
                    putHiveSiteProperty('javax.jdo.option.ConnectionURL', dbConnection)

        servicesList = self.get_services_list(services)
        if "PIG" in servicesList:
            ambari_user = self.getAmbariUser(services)
            ambariHostName = socket.getfqdn()
            webHcatSiteProperty = self.putProperty(configurations, "webhcat-site", services)
            webHcatSiteProperty("webhcat.proxyuser.{0}.hosts".format(ambari_user), ambariHostName)
            webHcatSiteProperty("webhcat.proxyuser.{0}.groups".format(ambari_user), "*")
            old_ambari_user = self.getOldAmbariUser(services)
            if old_ambari_user is not None:
                webHcatSitePropertyAttributes = self.putPropertyAttribute(configurations, "webhcat-site")
                webHcatSitePropertyAttributes("webhcat.proxyuser.{0}.hosts".format(old_ambari_user), 'delete', 'true')
                webHcatSitePropertyAttributes("webhcat.proxyuser.{0}.groups".format(old_ambari_user), 'delete', 'true')

        if self.is_secured_cluster(services):
            appendCoreSiteProperty = self.updateProperty(configurations, "core-site", services)

            def updateCallback(originalValue, newValue):
                """
                :type originalValue str
                :type newValue list
                """
                if originalValue and not originalValue.isspace():
                    hosts = originalValue.split(',')

                    if newValue:
                        hosts.extend(newValue)

                    result = ','.join(set(hosts))
                    return result
                else:
                    return ','.join(set(newValue))

            meta = self.get_service_component_meta("HIVE", "WEBHCAT_SERVER", services)
            if "hostnames" in meta:
                appendCoreSiteProperty('hadoop.proxyuser.HTTP.hosts', meta["hostnames"], updateCallback)

        # Recommend Ranger Hive authorization as per Ranger Hive plugin property
        if "ranger-env" in services["configurations"] and "hive-env" in services["configurations"] and \
                        "ranger-hive-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            rangerEnvHivePluginProperty = services["configurations"]["ranger-env"]["properties"][
                "ranger-hive-plugin-enabled"]
            if (rangerEnvHivePluginProperty.lower() == "yes"):
                putHiveEnvProperty("hive_security_authorization", "RANGER")

        # Security
        if ("configurations" not in services) or ("hive-env" not in services["configurations"]) or \
                ("properties" not in services["configurations"]["hive-env"]) or \
                ("hive_security_authorization" not in services["configurations"]["hive-env"]["properties"]) or \
                        str(services["configurations"]["hive-env"]["properties"][
                                "hive_security_authorization"]).lower() == "none":
            putHiveEnvProperty("hive_security_authorization", "None")
        else:
            putHiveEnvProperty("hive_security_authorization",
                               services["configurations"]["hive-env"]["properties"]["hive_security_authorization"])

        # Recommend Ranger Hive authorization as per Ranger Hive plugin property
        if "ranger-env" in services["configurations"] and "hive-env" in services["configurations"] and \
                        "ranger-hive-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            rangerEnvHivePluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-hive-plugin-enabled"]
            rangerEnvHiveAuthProperty = services["configurations"]["hive-env"]["properties"]["hive_security_authorization"]
            if (rangerEnvHivePluginProperty.lower() == "yes"):
                putHiveEnvProperty("hive_security_authorization", "Ranger")
            elif (rangerEnvHiveAuthProperty.lower() == "ranger"):
                putHiveEnvProperty("hive_security_authorization", "None")

        # hive_security_authorization == 'none'
        # this property is unrelated to Kerberos
        if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "none":
            putHiveSiteProperty("hive.security.authorization.manager",
                                "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory")
            if ("hive.security.authorization.manager" in configurations["hiveserver2-site"]["properties"]) or \
                    ("hiveserver2-site" not in services["configurations"]) or \
                    ("hiveserver2-site" in services["configurations"] and "hive.security.authorization.manager" in
                        services["configurations"]["hiveserver2-site"]["properties"]):
                putHiveServerPropertyAttribute("hive.security.authorization.manager", "delete", "true")
            if ("hive.security.authenticator.manager" in configurations["hiveserver2-site"]["properties"]) or \
                    ("hiveserver2-site" not in services["configurations"]) or \
                    ("hiveserver2-site" in services["configurations"] and "hive.security.authenticator.manager" in
                        services["configurations"]["hiveserver2-site"]["properties"]):
                putHiveServerPropertyAttribute("hive.security.authenticator.manager", "delete", "true")
            if ("hive.conf.restricted.list" in configurations["hiveserver2-site"]["properties"]) or \
                    ("hiveserver2-site" not in services["configurations"]) or \
                    ("hiveserver2-site" in services["configurations"] and "hive.conf.restricted.list" in
                        services["configurations"]["hiveserver2-site"]["properties"]):
                putHiveServerPropertyAttribute("hive.conf.restricted.list", "delete", "true")
            if "KERBEROS" not in servicesList:  # Kerberos security depends on this property
                putHiveSiteProperty("hive.security.authorization.enabled", "false")
        else:
            putHiveSiteProperty("hive.security.authorization.enabled", "true")

        try:
            auth_manager_value = str(
                configurations["hive-env"]["properties"]["hive.security.metastore.authorization.manager"])
        except KeyError:
            auth_manager_value = 'org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider'
            pass
        auth_manager_values = auth_manager_value.split(",")
        sqlstdauth_class = "org.apache.hadoop.hive.ql.security.authorization.MetaStoreAuthzAPIAuthorizerEmbedOnly"
        putHiveSiteProperty("hive.server2.enable.doAs", "true")

        # hive_security_authorization == 'sqlstdauth'
        if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "sqlstdauth":
            putHiveSiteProperty("hive.server2.enable.doAs", "false")
            putHiveServerProperty("hive.security.authorization.enabled", "true")
            putHiveServerProperty("hive.security.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory")
            putHiveServerProperty("hive.security.authenticator.manager",
                                      "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator")
            putHiveServerProperty("hive.conf.restricted.list",
                                      "hive.security.authenticator.manager,hive.security.authorization.manager,hive.users.in.admin.role")
            putHiveSiteProperty("hive.security.authorization.manager",
                                    "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory")
            if sqlstdauth_class not in auth_manager_values:
                auth_manager_values.append(sqlstdauth_class)
        elif sqlstdauth_class in auth_manager_values:
            # remove item from csv
            auth_manager_values = [x for x in auth_manager_values if x != sqlstdauth_class]
            pass
        putHiveSiteProperty("hive.security.metastore.authorization.manager", ",".join(auth_manager_values))

        # hive_security_authorization == 'ranger'
        if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "ranger":
            putHiveSiteProperty("hive.server2.enable.doAs", "false")
            putHiveServerProperty("hive.security.authorization.enabled", "true")
            putHiveServerProperty("hive.security.authorization.manager", "com.xasecure.authorization.hive.authorizer.XaSecureHiveAuthorizerFactory")
            putHiveServerProperty("hive.security.authenticator.manager", "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator")
            putHiveServerProperty("hive.conf.restricted.list", "hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager")

        # hive_security_authorization == 'ranger'
        if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "ranger":
            putHiveServerProperty("hive.security.authorization.manager",
                                  "org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory")


    def getAmsMemoryRecommendation(self, services, hosts):
        # MB per sink in hbase heapsize
        HEAP_PER_MASTER_COMPONENT = 50
        HEAP_PER_SLAVE_COMPONENT = 10

        schMemoryMap = {
            "HDFS": {
                "NAMENODE": HEAP_PER_MASTER_COMPONENT,
                "DATANODE": HEAP_PER_SLAVE_COMPONENT
            },
            "YARN": {
                "RESOURCEMANAGER": HEAP_PER_MASTER_COMPONENT,
            },
            "HBASE": {
                "HBASE_MASTER": HEAP_PER_MASTER_COMPONENT,
                "HBASE_REGIONSERVER": HEAP_PER_SLAVE_COMPONENT
            },
            "ACCUMULO": {
                "ACCUMULO_MASTER": HEAP_PER_MASTER_COMPONENT,
                "ACCUMULO_TSERVER": HEAP_PER_SLAVE_COMPONENT
            },
            "KAFKA": {
                "KAFKA_BROKER": HEAP_PER_MASTER_COMPONENT
            },
            "FLUME": {
                "FLUME_HANDLER": HEAP_PER_SLAVE_COMPONENT
            },
            "STORM": {
                "NIMBUS": HEAP_PER_MASTER_COMPONENT,
            },
            "AMBARI_METRICS": {
                "METRICS_COLLECTOR": HEAP_PER_MASTER_COMPONENT,
                "METRICS_MONITOR": HEAP_PER_SLAVE_COMPONENT
            }
        }
        total_sinks_count = 0
        # minimum heap size
        hbase_heapsize = 500
        for serviceName, componentsDict in schMemoryMap.items():
            for componentName, multiplier in componentsDict.items():
                schCount = len(
                    self.getHostsWithComponent(serviceName, componentName, services,
                                               hosts))
                hbase_heapsize += int((schCount * multiplier) ** 0.9)
                total_sinks_count += schCount
        collector_heapsize = int(hbase_heapsize/4 if hbase_heapsize > 2048 else 512)

        return round_to_n(collector_heapsize), round_to_n(hbase_heapsize), total_sinks_count

    def recommendStormConfigurations(self, configurations, clusterData, services, hosts):
        putStormSiteProperty = self.putProperty(configurations, "storm-site", services)
        putStormSiteAttributes = self.putPropertyAttribute(configurations, "storm-site")
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        # Storm AMS integration
        if 'AMBARI_METRICS' in servicesList:
            putStormSiteProperty('metrics.reporter.register', 'org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsReporter')
        # 2.2 stack
        storm_site = getServicesSiteProperties(services, "storm-site")
        security_enabled = (storm_site is not None and "storm.zookeeper.superACL" in storm_site)
        if "ranger-env" in services["configurations"] and "ranger-storm-plugin-properties" in services[
            "configurations"] and "ranger-storm-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            putStormRangerPluginProperty = self.putProperty(configurations, "ranger-storm-plugin-properties", services)
            rangerEnvStormPluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-storm-plugin-enabled"]
            putStormRangerPluginProperty("ranger-storm-plugin-enabled", rangerEnvStormPluginProperty)

        rangerPluginEnabled = ''
        if 'ranger-storm-plugin-properties' in configurations and 'ranger-storm-plugin-enabled' in configurations['ranger-storm-plugin-properties']['properties']:
            rangerPluginEnabled = configurations['ranger-storm-plugin-properties']['properties']['ranger-storm-plugin-enabled']
        elif 'ranger-storm-plugin-properties' in services['configurations'] and 'ranger-storm-plugin-enabled' in \
                services['configurations']['ranger-storm-plugin-properties']['properties']:
            rangerPluginEnabled = services['configurations']['ranger-storm-plugin-properties']['properties']['ranger-storm-plugin-enabled']

        storm_authorizer_class = 'org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer'
        ranger_authorizer_class = 'org.apache.ranger.authorization.storm.authorizer.RangerStormAuthorizer'
        # Cluster is kerberized
        if security_enabled:
            if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
                putStormSiteProperty('nimbus.authorizer', ranger_authorizer_class)
            elif rangerPluginEnabled and (rangerPluginEnabled.lower() == 'No'.lower()) and (
                services["configurations"]["storm-site"]["properties"]["nimbus.authorizer"] == ranger_authorizer_class):
                putStormSiteProperty('nimbus.authorizer', storm_authorizer_class)
        else:
            putStormSiteAttributes('nimbus.authorizer', 'delete', 'true')
        #2.5
        if security_enabled:
            _storm_principal_name = services['configurations']['storm-env']['properties']['storm_principal_name']
            storm_bare_jaas_principal = get_bare_principal(_storm_principal_name)
            if 'nimbus.impersonation.acl' in storm_site:
                storm_nimbus_impersonation_acl = storm_site["nimbus.impersonation.acl"]
                storm_nimbus_impersonation_acl.replace('{{storm_bare_jaas_principal}}', storm_bare_jaas_principal)
                putStormSiteProperty('nimbus.impersonation.acl', storm_nimbus_impersonation_acl)

    def validateStormRangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []
        ranger_plugin_properties = getSiteProperties(configurations, "ranger-storm-plugin-properties")
        ranger_plugin_enabled = ranger_plugin_properties['ranger-storm-plugin-enabled'] if ranger_plugin_properties else 'No'
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        security_enabled = self.isSecurityEnabled(services)
        if ranger_plugin_enabled.lower() == 'yes':
            # ranger-hdfs-plugin must be enabled in ranger-env
            ranger_env = getServicesSiteProperties(services, 'ranger-env')
            if not ranger_env or not 'ranger-storm-plugin-enabled' in ranger_env or ranger_env['ranger-storm-plugin-enabled'].lower() != 'yes':
                validationItems.append({"config-name": 'ranger-storm-plugin-enabled',
                                        "item": self.getWarnItem("ranger-storm-plugin-properties/ranger-storm-plugin-enabled must correspond ranger-env/ranger-storm-plugin-enabled")})
        if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()) and not security_enabled:
            validationItems.append({"config-name": "ranger-storm-plugin-enabled",
                                    "item": self.getWarnItem("Ranger Storm plugin should not be enabled in non-kerberos environment.")})

        return self.toConfigurationValidationProblems(validationItems, "ranger-storm-plugin-properties")

    def recommendAmsConfigurations(self, configurations, clusterData, services, hosts):
        putAmsEnvProperty = self.putProperty(configurations, "ams-env", services)
        putAmsHbaseSiteProperty = self.putProperty(configurations, "ams-hbase-site", services)
        putAmsSiteProperty = self.putProperty(configurations, "ams-site", services)
        putHbaseEnvProperty = self.putProperty(configurations, "ams-hbase-env", services)
        putGrafanaProperty = self.putProperty(configurations, "ams-grafana-env", services)
        putGrafanaPropertyAttribute = self.putPropertyAttribute(configurations, "ams-grafana-env")

        amsCollectorHosts = self.getComponentHostNames(services, "AMBARI_METRICS", "METRICS_COLLECTOR")

        if 'cluster-env' in services['configurations'] and \
                        'metrics_collector_vip_host' in services['configurations']['cluster-env']['properties']:
            metric_collector_host = services['configurations']['cluster-env']['properties']['metrics_collector_vip_host']
        else:
            metric_collector_host = 'localhost' if len(amsCollectorHosts) == 0 else amsCollectorHosts[0]

        putAmsSiteProperty("timeline.metrics.service.webapp.address", str(metric_collector_host) + ":6188")

        log_dir = "/var/log/ambari-metrics-collector"
        if "ams-env" in services["configurations"]:
            if "metrics_collector_log_dir" in services["configurations"]["ams-env"]["properties"]:
                log_dir = services["configurations"]["ams-env"]["properties"]["metrics_collector_log_dir"]
            putHbaseEnvProperty("hbase_log_dir", log_dir)

        defaultFs = 'file:///'
        if "core-site" in services["configurations"] and \
                        "fs.defaultFS" in services["configurations"]["core-site"]["properties"]:
            defaultFs = services["configurations"]["core-site"]["properties"]["fs.defaultFS"]

        operatingMode = "embedded"
        if "ams-site" in services["configurations"]:
            if "timeline.metrics.service.operation.mode" in services["configurations"]["ams-site"]["properties"]:
                operatingMode = services["configurations"]["ams-site"]["properties"]["timeline.metrics.service.operation.mode"]

        if operatingMode == "distributed":
            putAmsSiteProperty("timeline.metrics.service.watcher.disabled", 'true')
            putAmsHbaseSiteProperty("hbase.cluster.distributed", 'true')
        else:
            putAmsSiteProperty("timeline.metrics.service.watcher.disabled", 'false')
            putAmsHbaseSiteProperty("hbase.cluster.distributed", 'false')

        rootDir = "file:///var/lib/ambari-metrics-collector/hbase"
        tmpDir = "/var/lib/ambari-metrics-collector/hbase-tmp"
        zk_port_default = []
        if "ams-hbase-site" in services["configurations"]:
            if "hbase.rootdir" in services["configurations"]["ams-hbase-site"]["properties"]:
                rootDir = services["configurations"]["ams-hbase-site"]["properties"]["hbase.rootdir"]
            if "hbase.tmp.dir" in services["configurations"]["ams-hbase-site"]["properties"]:
                tmpDir = services["configurations"]["ams-hbase-site"]["properties"]["hbase.tmp.dir"]
            if "hbase.zookeeper.property.clientPort" in services["configurations"]["ams-hbase-site"]["properties"]:
                zk_port_default = services["configurations"]["ams-hbase-site"]["properties"]["hbase.zookeeper.property.clientPort"]

                # Skip recommendation item if default value is present
        if operatingMode == "distributed" and not "{{zookeeper_clientPort}}" in zk_port_default:
            zkPort = self.getZKPort(services)
            putAmsHbaseSiteProperty("hbase.zookeeper.property.clientPort", zkPort)
        elif operatingMode == "embedded" and not "{{zookeeper_clientPort}}" in zk_port_default:
            putAmsHbaseSiteProperty("hbase.zookeeper.property.clientPort", "61181")

        mountpoints = ["/"]
        for collectorHostName in amsCollectorHosts:
            for host in hosts["items"]:
                if host["Hosts"]["host_name"] == collectorHostName:
                    mountpoints = self.getPreferredMountPoints(host["Hosts"])
                    break
        isLocalRootDir = rootDir.startswith("file://") or (defaultFs.startswith("file://") and rootDir.startswith("/"))
        if isLocalRootDir:
            rootDir = re.sub("^file:///|/", "", rootDir, count=1)
            rootDir = "file://" + os.path.join(mountpoints[0], rootDir)

        if operatingMode == "distributed":
            putAmsHbaseSiteProperty("hbase.rootdir", defaultFs + "/user/ams/hbase")

        if operatingMode == "embedded":
            if isLocalRootDir:
                putAmsHbaseSiteProperty("hbase.rootdir", rootDir)
            else:
                putAmsHbaseSiteProperty("hbase.rootdir", "file:///var/lib/ambari-metrics-collector/hbase")

        collector_heapsize, hbase_heapsize, total_sinks_count = self.getAmsMemoryRecommendation(services, hosts)

        putAmsEnvProperty("metrics_collector_heapsize", collector_heapsize)

        # blockCache = 0.3, memstore = 0.35, phoenix-server = 0.15, phoenix-client = 0.25
        putAmsHbaseSiteProperty("hfile.block.cache.size", 0.3)
        putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size", 134217728)
        putAmsHbaseSiteProperty("hbase.regionserver.global.memstore.upperLimit", 0.35)
        putAmsHbaseSiteProperty("hbase.regionserver.global.memstore.lowerLimit", 0.3)

        if len(amsCollectorHosts) > 1:
            pass
        else:
            # blockCache = 0.3, memstore = 0.3, phoenix-server = 0.2, phoenix-client = 0.3
            if total_sinks_count >= 2000:
                putAmsHbaseSiteProperty("hbase.regionserver.handler.count", 60)
                putAmsHbaseSiteProperty("hbase.regionserver.hlog.blocksize", 134217728)
                putAmsHbaseSiteProperty("hbase.regionserver.maxlogs", 64)
                putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size", 268435456)
                putAmsHbaseSiteProperty("hbase.regionserver.global.memstore.upperLimit", 0.3)
                putAmsHbaseSiteProperty("hbase.regionserver.global.memstore.lowerLimit", 0.25)
                putAmsHbaseSiteProperty("phoenix.query.maxGlobalMemoryPercentage", 20)
                putAmsHbaseSiteProperty("phoenix.coprocessor.maxMetaDataCacheSize", 81920000)
                putAmsSiteProperty("phoenix.query.maxGlobalMemoryPercentage", 30)
                putAmsSiteProperty("timeline.metrics.service.resultset.fetchSize", 10000)
            elif total_sinks_count >= 500:
                putAmsHbaseSiteProperty("hbase.regionserver.handler.count", 60)
                putAmsHbaseSiteProperty("hbase.regionserver.hlog.blocksize", 134217728)
                putAmsHbaseSiteProperty("hbase.regionserver.maxlogs", 64)
                putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size", 268435456)
                putAmsHbaseSiteProperty("phoenix.coprocessor.maxMetaDataCacheSize", 40960000)
                putAmsSiteProperty("timeline.metrics.service.resultset.fetchSize", 5000)
            else:
                putAmsHbaseSiteProperty("phoenix.coprocessor.maxMetaDataCacheSize", 20480000)
            pass

        metrics_api_handlers = min(50, max(20, int(total_sinks_count / 100)))
        putAmsSiteProperty("timeline.metrics.service.handler.thread.count", metrics_api_handlers)

        # Distributed mode heap size
        if operatingMode == "distributed":
            hbase_heapsize = max(hbase_heapsize, 768)
            putHbaseEnvProperty("hbase_master_heapsize", "512")
            putHbaseEnvProperty("hbase_master_xmn_size", "102") #20% of 512 heap size
            putHbaseEnvProperty("hbase_regionserver_heapsize", hbase_heapsize)
            putHbaseEnvProperty("regionserver_xmn_size", round_to_n(0.15*hbase_heapsize,64))
        else:
            # Embedded mode heap size : master + regionserver
            hbase_rs_heapsize = 768
            putHbaseEnvProperty("hbase_regionserver_heapsize", hbase_rs_heapsize)
            putHbaseEnvProperty("hbase_master_heapsize", hbase_heapsize)
            putHbaseEnvProperty("hbase_master_xmn_size", round_to_n(0.15*(hbase_heapsize+hbase_rs_heapsize),64))

        # If no local DN in distributed mode
        if operatingMode == "distributed":
            dn_hosts = self.getComponentHostNames(services, "HDFS", "DATANODE")
            # call by Kerberos wizard sends only the service being affected
            # so it is possible for dn_hosts to be None but not amsCollectorHosts
            if dn_hosts and len(dn_hosts) > 0:
                if set(amsCollectorHosts).intersection(dn_hosts):
                    collector_cohosted_with_dn = "true"
                else:
                    collector_cohosted_with_dn = "false"
                putAmsHbaseSiteProperty("dfs.client.read.shortcircuit", collector_cohosted_with_dn)

        #split points
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        metricsDir = os.path.join(scriptDir, '../../../../common-services/AMBARI_METRICS/0.1.0/package')
        serviceMetricsDir = os.path.join(metricsDir, 'files', 'service-metrics')
        sys.path.append(os.path.join(metricsDir, 'scripts'))
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

        from split_points import FindSplitPointsForAMSRegions

        ams_hbase_site = None
        ams_hbase_env = None

        # Overriden properties form the UI
        if "ams-hbase-site" in services["configurations"]:
            ams_hbase_site = services["configurations"]["ams-hbase-site"]["properties"]
        if "ams-hbase-env" in services["configurations"]:
            ams_hbase_env = services["configurations"]["ams-hbase-env"]["properties"]

        # Recommendations
        if not ams_hbase_site:
            ams_hbase_site = configurations["ams-hbase-site"]["properties"]
        if not ams_hbase_env:
            ams_hbase_env = configurations["ams-hbase-env"]["properties"]

        split_point_finder = FindSplitPointsForAMSRegions(
            ams_hbase_site, ams_hbase_env, serviceMetricsDir, operatingMode, servicesList)

        result = split_point_finder.get_split_points()
        precision_splits = ' '
        aggregate_splits = ' '
        if result.precision:
            precision_splits = result.precision
        if result.aggregate:
            aggregate_splits = result.aggregate
        putAmsSiteProperty("timeline.metrics.host.aggregate.splitpoints", ','.join(precision_splits))
        putAmsSiteProperty("timeline.metrics.cluster.aggregate.splitpoints", ','.join(aggregate_splits))

        component_grafana_exists = False
        for service in services['services']:
            if 'components' in service:
                for component in service['components']:
                    if 'StackServiceComponents' in component:
                        # If Grafana is installed the hostnames would indicate its location
                        if 'METRICS_GRAFANA' in component['StackServiceComponents']['component_name'] and \
                                        len(component['StackServiceComponents']['hostnames']) != 0:
                            component_grafana_exists = True
                            break
        pass

        if not component_grafana_exists:
            putGrafanaPropertyAttribute("metrics_grafana_password", "visible", "false")

        pass

    def getHostNamesWithComponent(self, serviceName, componentName, services):
        """
        Returns the list of hostnames on which service component is installed
        """
        if services is not None and serviceName in [service["StackServices"]["service_name"] for service in services["services"]]:
            service = [serviceEntry for serviceEntry in services["services"] if serviceEntry["StackServices"]["service_name"] == serviceName][0]
            components = [componentEntry for componentEntry in service["components"] if componentEntry["StackServiceComponents"]["component_name"] == componentName]
            if (len(components) > 0 and len(components[0]["StackServiceComponents"]["hostnames"]) > 0):
                componentHostnames = components[0]["StackServiceComponents"]["hostnames"]
                return componentHostnames
        return []

    def getHostsWithComponent(self, serviceName, componentName, services, hosts):
        if services is not None and hosts is not None and serviceName in [service["StackServices"]["service_name"] for service in services["services"]]:
            service = [serviceEntry for serviceEntry in services["services"] if serviceEntry["StackServices"]["service_name"] == serviceName][0]
            components = [componentEntry for componentEntry in service["components"] if componentEntry["StackServiceComponents"]["component_name"] == componentName]
            if (len(components) > 0 and len(components[0]["StackServiceComponents"]["hostnames"]) > 0):
                componentHostnames = components[0]["StackServiceComponents"]["hostnames"]
                componentHosts = [host for host in hosts["items"] if host["Hosts"]["host_name"] in componentHostnames]
                return componentHosts
        return []

    def getHostWithComponent(self, serviceName, componentName, services, hosts):
        componentHosts = self.getHostsWithComponent(serviceName, componentName, services, hosts)
        if (len(componentHosts) > 0):
            return componentHosts[0]
        return None

    def getHostComponentsByCategories(self, hostname, categories, services, hosts):
        components = []
        if services is not None and hosts is not None:
            for service in services["services"]:
                components.extend([componentEntry for componentEntry in service["components"]
                                   if componentEntry["StackServiceComponents"]["component_category"] in categories
                                   and hostname in componentEntry["StackServiceComponents"]["hostnames"]])
        return components

    def getZKHostPortString(self, services, include_port=True):
        """
        Returns the comma delimited string of zookeeper server host with the configure port installed in a cluster
        Example: zk.host1.org:2181,zk.host2.org:2181,zk.host3.org:2181
        include_port boolean param -> If port is also needed.
        """
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        include_zookeeper = "ZOOKEEPER" in servicesList
        zookeeper_host_port = ''

        if include_zookeeper:
            zookeeper_hosts = self.getHostNamesWithComponent("ZOOKEEPER", "ZOOKEEPER_SERVER", services)
            zookeeper_host_port_arr = []

            if include_port:
                zookeeper_port = self.getZKPort(services)
                for i in range(len(zookeeper_hosts)):
                    zookeeper_host_port_arr.append(zookeeper_hosts[i] + ':' + zookeeper_port)
            else:
                for i in range(len(zookeeper_hosts)):
                    zookeeper_host_port_arr.append(zookeeper_hosts[i])

            zookeeper_host_port = ",".join(zookeeper_host_port_arr)
        return zookeeper_host_port

    def getZKPort(self, services):
        zookeeper_port = '2181'     #default port
        if 'zoo.cfg' in services['configurations'] and ('clientPort' in services['configurations']['zoo.cfg']['properties']):
            zookeeper_port = services['configurations']['zoo.cfg']['properties']['clientPort']
        return zookeeper_port

    def getConfigurationClusterSummary(self, servicesList, hosts, components, services):

        hBaseInstalled = False
        if 'HBASE' in servicesList:
            hBaseInstalled = True

        cluster = {
            "cpu": 0,
            "disk": 0,
            "ram": 0,
            "hBaseInstalled": hBaseInstalled,
            "components": components
        }

        if len(hosts["items"]) > 0:
            nodeManagerHosts = self.getHostsWithComponent("YARN", "NODEMANAGER", services, hosts)
            # NodeManager host with least memory is generally used in calculations as it will work in larger hosts.
            if nodeManagerHosts is not None and len(nodeManagerHosts) > 0:
                nodeManagerHost = nodeManagerHosts[0];
                for nmHost in nodeManagerHosts:
                    if nmHost["Hosts"]["total_mem"] < nodeManagerHost["Hosts"]["total_mem"]:
                        nodeManagerHost = nmHost
                host = nodeManagerHost["Hosts"]
                cluster["referenceNodeManagerHost"] = host
            else:
                host = hosts["items"][0]["Hosts"]
            cluster["referenceHost"] = host
            cluster["cpu"] = host["cpu_count"]
            cluster["disk"] = len(host["disk_info"])
            cluster["ram"] = int(host["total_mem"] / (1024 * 1024))

        ramRecommendations = [
            {"os":1, "hbase":1},
            {"os":2, "hbase":1},
            {"os":2, "hbase":2},
            {"os":4, "hbase":4},
            {"os":6, "hbase":8},
            {"os":8, "hbase":8},
            {"os":8, "hbase":8},
            {"os":12, "hbase":16},
            {"os":24, "hbase":24},
            {"os":32, "hbase":32},
            {"os":64, "hbase":32}
        ]
        index = {
            cluster["ram"] <= 4: 0,
            4 < cluster["ram"] <= 8: 1,
            8 < cluster["ram"] <= 16: 2,
            16 < cluster["ram"] <= 24: 3,
            24 < cluster["ram"] <= 48: 4,
            48 < cluster["ram"] <= 64: 5,
            64 < cluster["ram"] <= 72: 6,
            72 < cluster["ram"] <= 96: 7,
            96 < cluster["ram"] <= 128: 8,
            128 < cluster["ram"] <= 256: 9,
            256 < cluster["ram"]: 10
        }[1]


        cluster["reservedRam"] = ramRecommendations[index]["os"]
        cluster["hbaseRam"] = ramRecommendations[index]["hbase"]


        cluster["minContainerSize"] = {
            cluster["ram"] <= 4: 256,
            4 < cluster["ram"] <= 8: 512,
            8 < cluster["ram"] <= 24: 1024,
            24 < cluster["ram"]: 2048
        }[1]

        totalAvailableRam = cluster["ram"] - cluster["reservedRam"]
        if cluster["hBaseInstalled"]:
            totalAvailableRam -= cluster["hbaseRam"]
        cluster["totalAvailableRam"] = max(512, totalAvailableRam * 1024)
        '''containers = max(3, min (2*cores,min (1.8*DISKS,(Total available RAM) / MIN_CONTAINER_SIZE))))'''
        cluster["containers"] = round(max(3,
                                          min(2 * cluster["cpu"],
                                              min(ceil(1.8 * cluster["disk"]),
                                                  cluster["totalAvailableRam"] / cluster["minContainerSize"]))))

        '''ramPerContainers = max(2GB, RAM - reservedRam - hBaseRam) / containers'''
        cluster["ramPerContainer"] = abs(cluster["totalAvailableRam"] / cluster["containers"])
        '''If greater than 1GB, value will be in multiples of 512.'''
        if cluster["ramPerContainer"] > 1024:
            cluster["ramPerContainer"] = int(cluster["ramPerContainer"] / 512) * 512

        cluster["mapMemory"] = int(cluster["ramPerContainer"])
        cluster["reduceMemory"] = cluster["ramPerContainer"]
        cluster["amMemory"] = max(cluster["mapMemory"], cluster["reduceMemory"])

        return cluster

    def getServiceConfigurationValidators(self):
        return {
            "HDFS": { "hdfs-site": self.validateHDFSConfigurations,
                      "hadoop-env": self.validateHDFSConfigurationsEnv,
                      "ranger-hdfs-plugin-properties": self.validateHDFSRangerPluginConfigurations},
            "MAPREDUCE2": {"mapred-site": self.validateMapReduce2Configurations},
            "YARN": {"yarn-site": self.validateYARNConfigurations,
                     "yarn-env": self.validateYARNEnvConfigurations,
                     "ranger-yarn-plugin-properties": self.validateYARNRangerPluginConfigurations},
            "HIVE": {"hiveserver2-site": self.validateHiveServer2Configurations,
                     "hive-site": self.validateHiveConfigurations,
                     "hive-env": self.validateHiveConfigurationsEnv,
                     "webhcat-site": self.validateWebhcatConfigurations},
            "HBASE": {"hbase-env": self.validateHbaseEnvConfigurations,
                      "ranger-hbase-plugin-properties": self.validateHBASERangerPluginConfigurations},
            "STORM": {"storm-site": self.validateStormConfigurations,
                      "ranger-storm-plugin-properties": self.validateStormRangerPluginConfigurations},
            "RANGER": {"admin-properties": self.validateRangerAdminConfigurations,
                       "ranger-env": self.validateRangerConfigurationsEnv,
                       "ranger-tagsync-site": self.validateRangerTagsyncConfigurations},
            "AMBARI_METRICS": {"ams-hbase-site": self.validateAmsHbaseSiteConfigurations,
                               "ams-hbase-env": self.validateAmsHbaseEnvConfigurations,
                               "ams-site": self.validateAmsSiteConfigurations,
                               "ams-env": self.validateAmsEnvConfigurations}
        }

    def validateMinMax(self, items, recommendedDefaults, configurations):

        # required for casting to the proper numeric type before comparison
        def convertToNumber(number):
            try:
                return int(number)
            except ValueError:
                return float(number)

        for configName in configurations:
            validationItems = []
            if configName in recommendedDefaults and "property_attributes" in recommendedDefaults[configName]:
                for propertyName in recommendedDefaults[configName]["property_attributes"]:
                    if propertyName in configurations[configName]["properties"]:
                        if "maximum" in recommendedDefaults[configName]["property_attributes"][propertyName] and \
                                        propertyName in recommendedDefaults[configName]["properties"]:
                            userValue = convertToNumber(configurations[configName]["properties"][propertyName])
                            maxValue = convertToNumber(recommendedDefaults[configName]["property_attributes"][propertyName]["maximum"])
                            if userValue > maxValue:
                                validationItems.extend([{"config-name": propertyName, "item": self.getWarnItem("Value is greater than the recommended maximum of {0} ".format(maxValue))}])
                        if "minimum" in recommendedDefaults[configName]["property_attributes"][propertyName] and \
                                        propertyName in recommendedDefaults[configName]["properties"]:
                            userValue = convertToNumber(configurations[configName]["properties"][propertyName])
                            minValue = convertToNumber(recommendedDefaults[configName]["property_attributes"][propertyName]["minimum"])
                            if userValue < minValue:
                                validationItems.extend([{"config-name": propertyName, "item": self.getWarnItem("Value is less than the recommended minimum of {0} ".format(minValue))}])
            items.extend(self.toConfigurationValidationProblems(validationItems, configName))
        pass

    def validateAmsSiteConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []

        op_mode = properties.get("timeline.metrics.service.operation.mode")
        correct_op_mode_item = None
        if op_mode not in ("embedded", "distributed"):
            correct_op_mode_item = self.getErrorItem("Correct value should be set.")
            pass

        validationItems.extend([{"config-name":'timeline.metrics.service.operation.mode', "item": correct_op_mode_item }])
        return self.toConfigurationValidationProblems(validationItems, "ams-site")

    def validateAmsHbaseSiteConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):

        amsCollectorHosts = self.getComponentHostNames(services, "AMBARI_METRICS", "METRICS_COLLECTOR")
        ams_site = getSiteProperties(configurations, "ams-site")
        core_site = getSiteProperties(configurations, "core-site")

        collector_heapsize, hbase_heapsize, total_sinks_count = self.getAmsMemoryRecommendation(services, hosts)
        recommendedDiskSpace = 10485760
        # TODO validate configuration for multiple AMBARI_METRICS collectors
        if len(amsCollectorHosts) > 1:
            pass
        else:
            if total_sinks_count > 2000:
                recommendedDiskSpace  = 104857600  # * 1k == 100 Gb
            elif total_sinks_count > 500:
                recommendedDiskSpace  = 52428800  # * 1k == 50 Gb
            elif total_sinks_count > 250:
                recommendedDiskSpace  = 20971520  # * 1k == 20 Gb

        validationItems = []

        rootdir_item = None
        op_mode = ams_site.get("timeline.metrics.service.operation.mode")
        default_fs = core_site.get("fs.defaultFS") if core_site else "file:///"
        hbase_rootdir = properties.get("hbase.rootdir")
        hbase_tmpdir = properties.get("hbase.tmp.dir")
        distributed = properties.get("hbase.cluster.distributed")
        is_local_root_dir = hbase_rootdir.startswith("file://") or (default_fs.startswith("file://") and hbase_rootdir.startswith("/"))

        if op_mode == "distributed" and is_local_root_dir:
            rootdir_item = self.getWarnItem("In distributed mode hbase.rootdir should point to HDFS.")
        elif op_mode == "embedded":
            if distributed.lower() == "false" and hbase_rootdir.startswith('/') or hbase_rootdir.startswith("hdfs://"):
                rootdir_item = self.getWarnItem("In embedded mode hbase.rootdir cannot point to schemaless values or HDFS, "
                                                "Example - file:// for localFS")
            pass

        distributed_item = None
        if op_mode == "distributed" and not distributed.lower() == "true":
            distributed_item = self.getErrorItem("hbase.cluster.distributed property should be set to true for "
                                                 "distributed mode")
        if op_mode == "embedded" and distributed.lower() == "true":
            distributed_item = self.getErrorItem("hbase.cluster.distributed property should be set to false for embedded mode")

        hbase_zk_client_port = properties.get("hbase.zookeeper.property.clientPort")
        zkPort = self.getZKPort(services)
        hbase_zk_client_port_item = None
        if distributed.lower() == "true" and op_mode == "distributed" and \
                        hbase_zk_client_port != zkPort and hbase_zk_client_port != "{{zookeeper_clientPort}}":
            hbase_zk_client_port_item = self.getErrorItem("In AMS distributed mode, hbase.zookeeper.property.clientPort "
                                                          "should be the cluster zookeeper server port : {0}".format(zkPort))

        if distributed.lower() == "false" and op_mode == "embedded" and \
                        hbase_zk_client_port == zkPort and hbase_zk_client_port != "{{zookeeper_clientPort}}":
            hbase_zk_client_port_item = self.getErrorItem("In AMS embedded mode, hbase.zookeeper.property.clientPort "
                                                          "should be a different port than cluster zookeeper port."
                                                          "(default:61181)")

        validationItems.extend([{"config-name":'hbase.rootdir', "item": rootdir_item },
                                {"config-name":'hbase.cluster.distributed', "item": distributed_item },
                                {"config-name":'hbase.zookeeper.property.clientPort', "item": hbase_zk_client_port_item }])

        for collectorHostName in amsCollectorHosts:
            for host in hosts["items"]:
                if host["Hosts"]["host_name"] == collectorHostName:
                    if op_mode == 'embedded' or is_local_root_dir:
                        validationItems.extend([{"config-name": 'hbase.rootdir', "item": self.validatorEnoughDiskSpace(properties, 'hbase.rootdir', host["Hosts"], recommendedDiskSpace)}])
                        validationItems.extend([{"config-name": 'hbase.rootdir', "item": self.validatorNotRootFs(properties, recommendedDefaults, 'hbase.rootdir', host["Hosts"])}])
                        validationItems.extend([{"config-name": 'hbase.tmp.dir', "item": self.validatorNotRootFs(properties, recommendedDefaults, 'hbase.tmp.dir', host["Hosts"])}])

                    dn_hosts = self.getComponentHostNames(services, "HDFS", "DATANODE")
                    if is_local_root_dir:
                        mountPoints = []
                        for mountPoint in host["Hosts"]["disk_info"]:
                            mountPoints.append(mountPoint["mountpoint"])
                        hbase_rootdir_mountpoint = getMountPointForDir(hbase_rootdir, mountPoints)
                        hbase_tmpdir_mountpoint = getMountPointForDir(hbase_tmpdir, mountPoints)
                        preferred_mountpoints = self.getPreferredMountPoints(host['Hosts'])
                        # hbase.rootdir and hbase.tmp.dir shouldn't point to the same partition
                        # if multiple preferred_mountpoints exist
                        if hbase_rootdir_mountpoint == hbase_tmpdir_mountpoint and \
                                        len(preferred_mountpoints) > 1:
                            item = self.getWarnItem("Consider not using {0} partition for storing metrics temporary data. "
                                                    "{0} partition is already used as hbase.rootdir to store metrics data".format(hbase_tmpdir_mountpoint))
                            validationItems.extend([{"config-name":'hbase.tmp.dir', "item": item}])

                        # if METRICS_COLLECTOR is co-hosted with DATANODE
                        # cross-check dfs.datanode.data.dir and hbase.rootdir
                        # they shouldn't share same disk partition IO
                        hdfs_site = getSiteProperties(configurations, "hdfs-site")
                        dfs_datadirs = hdfs_site.get("dfs.datanode.data.dir").split(",") if hdfs_site and "dfs.datanode.data.dir" in hdfs_site else []
                        if dn_hosts and collectorHostName in dn_hosts and ams_site and \
                                dfs_datadirs and len(preferred_mountpoints) > len(dfs_datadirs):
                            for dfs_datadir in dfs_datadirs:
                                dfs_datadir_mountpoint = getMountPointForDir(dfs_datadir, mountPoints)
                                if dfs_datadir_mountpoint == hbase_rootdir_mountpoint:
                                    item = self.getWarnItem("Consider not using {0} partition for storing metrics data. "
                                                            "{0} is already used by datanode to store HDFS data".format(hbase_rootdir_mountpoint))
                                    validationItems.extend([{"config-name": 'hbase.rootdir', "item": item}])
                                    break
                    # If no local DN in distributed mode
                    elif collectorHostName not in dn_hosts and distributed.lower() == "true":
                        item = self.getWarnItem("It's recommended to install Datanode component on {0} "
                                                "to speed up IO operations between HDFS and Metrics "
                                                "Collector in distributed mode ".format(collectorHostName))
                        validationItems.extend([{"config-name": "hbase.cluster.distributed", "item": item}])
                    # Short circuit read should be enabled in distibuted mode
                    # if local DN installed
                    else:
                        validationItems.extend([{"config-name": "dfs.client.read.shortcircuit", "item": self.validatorEqualsToRecommendedItem(properties, recommendedDefaults, "dfs.client.read.shortcircuit")}])

        return self.toConfigurationValidationProblems(validationItems, "ams-hbase-site")

    def validateStormConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        # Storm AMS integration
        if 'AMBARI_METRICS' in servicesList and "metrics.reporter.register" in properties and \
                        "org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsReporter" not in properties.get("metrics.reporter.register"):

            validationItems.append({"config-name": 'metrics.reporter.register',
                                    "item": self.getWarnItem(
                                        "Should be set to org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsReporter to report the metrics to Ambari Metrics service.")})

        return self.toConfigurationValidationProblems(validationItems, "storm-site")

    def validateAmsHbaseEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):

        ams_env = getSiteProperties(configurations, "ams-env")
        amsHbaseSite = getSiteProperties(configurations, "ams-hbase-site")
        validationItems = []
        mb = 1024 * 1024
        gb = 1024 * mb

        regionServerItem = self.validatorLessThenDefaultValue(properties, recommendedDefaults, "hbase_regionserver_heapsize") ## FIXME if new service added
        if regionServerItem:
            validationItems.extend([{"config-name": "hbase_regionserver_heapsize", "item": regionServerItem}])

        hbaseMasterHeapsizeItem = self.validatorLessThenDefaultValue(properties, recommendedDefaults, "hbase_master_heapsize")
        if hbaseMasterHeapsizeItem:
            validationItems.extend([{"config-name": "hbase_master_heapsize", "item": hbaseMasterHeapsizeItem}])

        logDirItem = self.validatorEqualsPropertyItem(properties, "hbase_log_dir", ams_env, "metrics_collector_log_dir")
        if logDirItem:
            validationItems.extend([{"config-name": "hbase_log_dir", "item": logDirItem}])

        hbase_master_heapsize = to_number(properties["hbase_master_heapsize"])
        hbase_master_xmn_size = to_number(properties["hbase_master_xmn_size"])
        hbase_regionserver_heapsize = to_number(properties["hbase_regionserver_heapsize"])
        hbase_regionserver_xmn_size = to_number(properties["regionserver_xmn_size"])

        # Validate Xmn settings.
        masterXmnItem = None
        regionServerXmnItem = None
        is_hbase_distributed = amsHbaseSite.get("hbase.cluster.distributed").lower() == 'true'

        if is_hbase_distributed:
            minMasterXmn = 0.12 * hbase_master_heapsize
            maxMasterXmn = 0.2 * hbase_master_heapsize
            if hbase_master_xmn_size < minMasterXmn:
                masterXmnItem = self.getWarnItem("Value is lesser than the recommended minimum Xmn size of {0} "
                                                 "(12% of hbase_master_heapsize)".format(int(ceil(minMasterXmn))))

            if hbase_master_xmn_size > maxMasterXmn:
                masterXmnItem = self.getWarnItem("Value is greater than the recommended maximum Xmn size of {0} "
                                                 "(20% of hbase_master_heapsize)".format(int(floor(maxMasterXmn))))

            minRegionServerXmn = 0.12 * hbase_regionserver_heapsize
            maxRegionServerXmn = 0.2 * hbase_regionserver_heapsize
            if hbase_regionserver_xmn_size < minRegionServerXmn:
                regionServerXmnItem = self.getWarnItem("Value is lesser than the recommended minimum Xmn size of {0} "
                                                       "(12% of hbase_regionserver_heapsize)"
                                                       .format(int(ceil(minRegionServerXmn))))

            if hbase_regionserver_xmn_size > maxRegionServerXmn:
                regionServerXmnItem = self.getWarnItem("Value is greater than the recommended maximum Xmn size of {0} "
                                                       "(20% of hbase_regionserver_heapsize)"
                                                       .format(int(floor(maxRegionServerXmn))))
        else:
            minMasterXmn = 0.12 * (hbase_master_heapsize + hbase_regionserver_heapsize)
            maxMasterXmn = 0.2 *  (hbase_master_heapsize + hbase_regionserver_heapsize)
            if hbase_master_xmn_size < minMasterXmn:
                masterXmnItem = self.getWarnItem("Value is lesser than the recommended minimum Xmn size of {0} "
                                                 "(12% of hbase_master_heapsize + hbase_regionserver_heapsize)"
                                                 .format(int(ceil(minMasterXmn))))

            if hbase_master_xmn_size > maxMasterXmn:
                masterXmnItem = self.getWarnItem("Value is greater than the recommended maximum Xmn size of {0} "
                                                 "(20% of hbase_master_heapsize + hbase_regionserver_heapsize)"
                                                 .format(int(floor(maxMasterXmn))))
        if masterXmnItem:
            validationItems.extend([{"config-name": "hbase_master_xmn_size", "item": masterXmnItem}])

        if regionServerXmnItem:
            validationItems.extend([{"config-name": "regionserver_xmn_size", "item": regionServerXmnItem}])

        if hbaseMasterHeapsizeItem is None:
            hostMasterComponents = {}

            for service in services["services"]:
                for component in service["components"]:
                    if component["StackServiceComponents"]["hostnames"] is not None:
                        for hostName in component["StackServiceComponents"]["hostnames"]:
                            if self.isMasterComponent(component):
                                if hostName not in hostMasterComponents.keys():
                                    hostMasterComponents[hostName] = []
                                hostMasterComponents[hostName].append(component["StackServiceComponents"]["component_name"])

            amsCollectorHosts = self.getComponentHostNames(services, "AMBARI_METRICS", "METRICS_COLLECTOR")
            for collectorHostName in amsCollectorHosts:
                for host in hosts["items"]:
                    if host["Hosts"]["host_name"] == collectorHostName:
                        # AMS Collector co-hosted with other master components in bigger clusters
                        if len(hosts['items']) > 31 and \
                                        len(hostMasterComponents[collectorHostName]) > 2 and \
                                        host["Hosts"]["total_mem"] < 32*mb: # < 32Gb(total_mem in k)
                            masterHostMessage = "Host {0} is used by multiple master components ({1}). " \
                                                "It is recommended to use a separate host for the " \
                                                "Ambari Metrics Collector component and ensure " \
                                                "the host has sufficient memory available."

                            hbaseMasterHeapsizeItem = self.getWarnItem(masterHostMessage.format(
                                collectorHostName, str(", ".join(hostMasterComponents[collectorHostName]))))
                            if hbaseMasterHeapsizeItem:
                                validationItems.extend([{"config-name": "hbase_master_heapsize", "item": hbaseMasterHeapsizeItem}])

                        # Check for unused RAM on AMS Collector node
                        hostComponents = []
                        for service in services["services"]:
                            for component in service["components"]:
                                if component["StackServiceComponents"]["hostnames"] is not None:
                                    if collectorHostName in component["StackServiceComponents"]["hostnames"]:
                                        hostComponents.append(component["StackServiceComponents"]["component_name"])

                        requiredMemory = getMemorySizeRequired(hostComponents, configurations)
                        unusedMemory = host["Hosts"]["total_mem"] * 1024 - requiredMemory # in bytes

                        heapPropertyToIncrease = "hbase_regionserver_heapsize" if is_hbase_distributed else "hbase_master_heapsize"
                        xmnPropertyToIncrease = "regionserver_xmn_size" if is_hbase_distributed else "hbase_master_xmn_size"
                        hbase_needs_increase = to_number(properties[heapPropertyToIncrease]) * mb < 32 * gb

                        if unusedMemory > 4*gb and hbase_needs_increase:  # warn user, if more than 4GB RAM is unused

                            recommended_hbase_heapsize = int((unusedMemory - 4*gb)*4/5) + to_number(properties.get(heapPropertyToIncrease))*mb
                            recommended_hbase_heapsize = min(32*gb, recommended_hbase_heapsize) #Make sure heapsize <= 32GB
                            recommended_hbase_heapsize = round_to_n(recommended_hbase_heapsize/mb,128) # Round to 128m multiple
                            if to_number(properties[heapPropertyToIncrease]) < recommended_hbase_heapsize:
                                hbaseHeapsizeItem = self.getWarnItem("Consider allocating {0} MB to {1} in ams-hbase-env to use up some "
                                                                     "unused memory on host"
                                                                     .format(recommended_hbase_heapsize,
                                                                             heapPropertyToIncrease))
                                validationItems.extend([{"config-name": heapPropertyToIncrease, "item": hbaseHeapsizeItem}])

                            recommended_xmn_size = round_to_n(0.15*recommended_hbase_heapsize,128)
                            if to_number(properties[xmnPropertyToIncrease]) < recommended_xmn_size:
                                xmnPropertyToIncreaseItem = self.getWarnItem("Consider allocating {0} MB to use up some unused memory "
                                                                             "on host".format(recommended_xmn_size))
                                validationItems.extend([{"config-name": xmnPropertyToIncrease, "item": xmnPropertyToIncreaseItem}])
            pass

        return self.toConfigurationValidationProblems(validationItems, "ams-hbase-env")

    def validateAmsEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):

        ams_env = getSiteProperties(configurations, "ams-env")
        mb = 1024 * 1024
        gb = 1024 * mb
        validationItems = []
        collector_heapsize = to_number(ams_env.get("metrics_collector_heapsize"))
        amsCollectorHosts = self.getComponentHostNames(services, "AMBARI_METRICS", "METRICS_COLLECTOR")
        for collectorHostName in amsCollectorHosts:
            for host in hosts["items"]:
                if host["Hosts"]["host_name"] == collectorHostName:
                    hostComponents = []
                    for service in services["services"]:
                        for component in service["components"]:
                            if component["StackServiceComponents"]["hostnames"] is not None:
                                if collectorHostName in component["StackServiceComponents"]["hostnames"]:
                                    hostComponents.append(component["StackServiceComponents"]["component_name"])

                    requiredMemory = getMemorySizeRequired(hostComponents, configurations)
                    unusedMemory = host["Hosts"]["total_mem"] * 1024 - requiredMemory # in bytes
                    collector_needs_increase = collector_heapsize * mb < 16 * gb

                    if unusedMemory > 4*gb and collector_needs_increase:  # warn user, if more than 4GB RAM is unused
                        recommended_collector_heapsize = int((unusedMemory - 4*gb)/5) + collector_heapsize * mb
                        recommended_collector_heapsize = round_to_n(recommended_collector_heapsize/mb,128) # Round to 128m multiple
                        if collector_heapsize < recommended_collector_heapsize:
                            validation_msg = "Consider allocating {0} MB to metrics_collector_heapsize in ams-env to use up some " \
                                             "unused memory on host"
                            collectorHeapsizeItem = self.getWarnItem(validation_msg.format(recommended_collector_heapsize))
                            validationItems.extend([{"config-name": "metrics_collector_heapsize", "item": collectorHeapsizeItem}])
        pass
        return self.toConfigurationValidationProblems(validationItems, "ams-env")

    def getPreferredMountPoints(self, hostInfo):

        # '/etc/resolv.conf', '/etc/hostname', '/etc/hosts' are docker specific mount points
        undesirableMountPoints = ["/", "/home", "/etc/resolv.conf", "/etc/hosts",
                                  "/etc/hostname", "/tmp"]
        undesirableFsTypes = ["devtmpfs", "tmpfs", "vboxsf", "CDFS"]
        mountPoints = []
        if hostInfo and "disk_info" in hostInfo:
            mountPointsDict = {}
            for mountpoint in hostInfo["disk_info"]:
                if not (mountpoint["mountpoint"] in undesirableMountPoints or
                            mountpoint["mountpoint"].startswith(("/boot", "/mnt")) or
                                mountpoint["type"] in undesirableFsTypes or
                                mountpoint["available"] == str(0)):
                    mountPointsDict[mountpoint["mountpoint"]] = to_number(mountpoint["available"])
            if mountPointsDict:
                mountPoints = sorted(mountPointsDict, key=mountPointsDict.get, reverse=True)
        mountPoints.append("/")
        return mountPoints

    def validatorNotRootFs(self, properties, recommendedDefaults, propertyName, hostInfo):
        if not propertyName in properties:
            return self.getErrorItem("Value should be set")
        dir = properties[propertyName]
        if not dir.startswith("file://") or dir == recommendedDefaults.get(propertyName):
            return None

        dir = re.sub("^file://", "", dir, count=1)
        mountPoints = []
        for mountPoint in hostInfo["disk_info"]:
            mountPoints.append(mountPoint["mountpoint"])
        mountPoint = getMountPointForDir(dir, mountPoints)

        if "/" == mountPoint and self.getPreferredMountPoints(hostInfo)[0] != mountPoint:
            return self.getWarnItem("It is not recommended to use root partition for {0}".format(propertyName))

        return None

    def validatorEnoughDiskSpace(self, properties, propertyName, hostInfo, reqiuredDiskSpace):
        if not propertyName in properties:
            return self.getErrorItem("Value should be set")
        dir = properties[propertyName]
        if not dir.startswith("file://"):
            return None

        dir = re.sub("^file://", "", dir, count=1)
        mountPoints = {}
        for mountPoint in hostInfo["disk_info"]:
            mountPoints[mountPoint["mountpoint"]] = to_number(mountPoint["available"])
        mountPoint = getMountPointForDir(dir, mountPoints.keys())

        if not mountPoints:
            return self.getErrorItem("No disk info found on host %s" % hostInfo["host_name"])

        if mountPoints[mountPoint] < reqiuredDiskSpace:
            msg = "Ambari Metrics disk space requirements not met. \n" \
                  "Recommended disk space for partition {0} is {1}G"
            return self.getWarnItem(msg.format(mountPoint, reqiuredDiskSpace/1048576)) # in Gb
        return None

    def validatorLessThenDefaultValue(self, properties, recommendedDefaults, propertyName):
        if propertyName not in recommendedDefaults:
            # If a property name exists in say hbase-env and hbase-site (which is allowed), then it will exist in the
            # "properties" dictionary, but not necessarily in the "recommendedDefaults" dictionary". In this case, ignore it.
            return None

        if not propertyName in properties:
            return self.getErrorItem("Value should be set")
        value = to_number(properties[propertyName])
        if value is None:
            return self.getErrorItem("Value should be integer")
        defaultValue = to_number(recommendedDefaults[propertyName])
        if defaultValue is None:
            return None
        if value < defaultValue:
            return self.getWarnItem("Value is less than the recommended default of {0}".format(defaultValue))
        return None

    def validatorEqualsPropertyItem(self, properties1, propertyName1,
                                    properties2, propertyName2,
                                    emptyAllowed=False):
        if not propertyName1 in properties1:
            return self.getErrorItem("Value should be set for %s" % propertyName1)
        if not propertyName2 in properties2:
            return self.getErrorItem("Value should be set for %s" % propertyName2)
        value1 = properties1.get(propertyName1)
        if value1 is None and not emptyAllowed:
            return self.getErrorItem("Empty value for %s" % propertyName1)
        value2 = properties2.get(propertyName2)
        if value2 is None and not emptyAllowed:
            return self.getErrorItem("Empty value for %s" % propertyName2)
        if value1 != value2:
            return self.getWarnItem("It is recommended to set equal values "
                                    "for properties {0} and {1}".format(propertyName1, propertyName2))

        return None

    def validatorEqualsToRecommendedItem(self, properties, recommendedDefaults,
                                         propertyName):
        if not propertyName in properties:
            return self.getErrorItem("Value should be set for %s" % propertyName)
        value = properties.get(propertyName)
        if not propertyName in recommendedDefaults:
            return self.getErrorItem("Value should be recommended for %s" % propertyName)
        recommendedValue = recommendedDefaults.get(propertyName)
        if value != recommendedValue:
            return self.getWarnItem("It is recommended to set value {0} "
                                    "for property {1}".format(recommendedValue, propertyName))
        return None

    def validateMinMemorySetting(self, properties, defaultValue, propertyName):
        if not propertyName in properties:
            return self.getErrorItem("Value should be set")
        if defaultValue is None:
            return self.getErrorItem("Config's default value can't be null or undefined")

        value = properties[propertyName]
        if value is None:
            return self.getErrorItem("Value can't be null or undefined")
        try:
            valueInt = to_number(value)
            # TODO: generify for other use cases
            defaultValueInt = int(str(defaultValue).strip())
            if valueInt < defaultValueInt:
                return self.getWarnItem("Value is less than the minimum recommended default of -Xmx" + str(defaultValue))
        except:
            return None

        return None

    def validatorYarnQueue(self, properties, recommendedDefaults, propertyName, services):
        if propertyName not in properties:
            return self.getErrorItem("Value should be set")

        capacity_scheduler_properties, _ = self.getCapacitySchedulerProperties(services)
        leaf_queue_names = self.getAllYarnLeafQueues(capacity_scheduler_properties)
        queue_name = properties[propertyName]

        if len(leaf_queue_names) == 0:
            return None
        elif queue_name not in leaf_queue_names:
            return self.getErrorItem("Queue is not exist or not corresponds to existing YARN leaf queue")

        return None

    def recommendYarnQueue(self, services, catalog_name=None, queue_property=None):
        old_queue_name = None

        if services and 'configurations' in services:
            configurations = services["configurations"]
            if catalog_name in configurations and queue_property in configurations[catalog_name]["properties"]:
                old_queue_name = configurations[catalog_name]["properties"][queue_property]

            capacity_scheduler_properties, _ = self.getCapacitySchedulerProperties(services)
            leaf_queues = sorted(self.getAllYarnLeafQueues(capacity_scheduler_properties))

            if leaf_queues and (old_queue_name is None or old_queue_name not in leaf_queues):
                return leaf_queues.pop()
            elif old_queue_name and old_queue_name in leaf_queues:
                return None

        return "default"

    def validateXmxValue(self, properties, recommendedDefaults, propertyName):
        if not propertyName in properties:
            return self.getErrorItem("Value should be set")
        value = properties[propertyName]
        defaultValue = recommendedDefaults[propertyName]
        if defaultValue is None:
            return self.getErrorItem("Config's default value can't be null or undefined")
        if not checkXmxValueFormat(value) and checkXmxValueFormat(defaultValue):
            # Xmx is in the default-value but not the value, should be an error
            return self.getErrorItem('Invalid value format')
        if not checkXmxValueFormat(defaultValue):
            # if default value does not contain Xmx, then there is no point in validating existing value
            return None
        valueInt = formatXmxSizeToBytes(getXmxSize(value))
        defaultValueXmx = getXmxSize(defaultValue)
        defaultValueInt = formatXmxSizeToBytes(defaultValueXmx)
        if valueInt < defaultValueInt:
            return self.getWarnItem("Value is less than the recommended default of -Xmx" + defaultValueXmx)
        return None

    def validateMapReduce2Configurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = [ {"config-name": 'mapreduce.map.java.opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'mapreduce.map.java.opts')},
                            {"config-name": 'mapreduce.reduce.java.opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'mapreduce.reduce.java.opts')},
                            {"config-name": 'mapreduce.task.io.sort.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.task.io.sort.mb')},
                            {"config-name": 'mapreduce.map.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.map.memory.mb')},
                            {"config-name": 'mapreduce.reduce.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.reduce.memory.mb')},
                            {"config-name": 'yarn.app.mapreduce.am.resource.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.app.mapreduce.am.resource.mb')},
                            {"config-name": 'yarn.app.mapreduce.am.command-opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'yarn.app.mapreduce.am.command-opts')},
                            {"config-name": 'mapreduce.job.queuename', "item": self.validatorYarnQueue(properties, recommendedDefaults, 'mapreduce.job.queuename', services)} ]
        return self.toConfigurationValidationProblems(validationItems, "mapred-site")

    def validateYARNConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        clusterEnv = getSiteProperties(configurations, "cluster-env")
        validationItems = [ {"config-name": 'yarn.nodemanager.resource.memory-mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.nodemanager.resource.memory-mb')},
                            {"config-name": 'yarn.scheduler.minimum-allocation-mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.scheduler.minimum-allocation-mb')},
                            {"config-name": 'yarn.nodemanager.linux-container-executor.group', "item": self.validatorEqualsPropertyItem(properties, "yarn.nodemanager.linux-container-executor.group", clusterEnv, "user_group")},
                            {"config-name": 'yarn.scheduler.maximum-allocation-mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.scheduler.maximum-allocation-mb')} ]
        yarn_site = properties
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        if 'RANGER_KMS' in servicesList and 'KERBEROS' in servicesList:
            yarn_resource_proxy_enabled = yarn_site['yarn.resourcemanager.proxy-user-privileges.enabled']
            if yarn_resource_proxy_enabled.lower() == 'true':
                validationItems.append({"config-name": 'yarn.resourcemanager.proxy-user-privileges.enabled',
                                        "item": self.getWarnItem(
                                            "If Ranger KMS service is installed set yarn.resourcemanager.proxy-user-privileges.enabled " \
                                            "property value as false under yarn-site"
                                            )})
        return self.toConfigurationValidationProblems(validationItems, "yarn-site")

    def validateYARNEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = [{"config-name": 'service_check.queue.name', "item": self.validatorYarnQueue(properties, recommendedDefaults, 'service_check.queue.name', services)} ]
        return self.toConfigurationValidationProblems(validationItems, "yarn-env")

    def validateYARNRangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []
        ranger_plugin_properties = getSiteProperties(configurations, "ranger-yarn-plugin-properties")
        ranger_plugin_enabled = ranger_plugin_properties['ranger-yarn-plugin-enabled'] if ranger_plugin_properties else 'No'
        if ranger_plugin_enabled.lower() == 'yes':
            # ranger-hdfs-plugin must be enabled in ranger-env
            ranger_env = getServicesSiteProperties(services, 'ranger-env')
            if not ranger_env or not 'ranger-yarn-plugin-enabled' in ranger_env or ranger_env['ranger-yarn-plugin-enabled'].lower() != 'yes':
                validationItems.append({"config-name": 'ranger-yarn-plugin-enabled',
                                        "item": self.getWarnItem(
                                            "ranger-yarn-plugin-properties/ranger-yarn-plugin-enabled must correspond ranger-env/ranger-yarn-plugin-enabled")})
        return self.toConfigurationValidationProblems(validationItems, "ranger-yarn-plugin-properties")

    def validateRangerConfigurationsEnv(self, properties, recommendedDefaults, configurations, services, hosts):
        ranger_env_properties = properties
        validationItems = []
        security_enabled = self.isSecurityEnabled(services)

        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        if "ranger-storm-plugin-enabled" in ranger_env_properties and ranger_env_properties[
            'ranger-storm-plugin-enabled'].lower() == 'yes' and not 'KERBEROS' in servicesList:
            validationItems.append({"config-name": "ranger-storm-plugin-enabled",
                                    "item": self.getWarnItem(
                                        "Ranger Storm plugin should not be enabled in non-kerberos environment.")})
        if "ranger-kafka-plugin-enabled" in ranger_env_properties and ranger_env_properties[
            "ranger-kafka-plugin-enabled"].lower() == 'yes' and not security_enabled:
            validationItems.append({"config-name": "ranger-kafka-plugin-enabled",
                                    "item": self.getWarnItem(
                                        "Ranger Kafka plugin should not be enabled in non-kerberos environment.")})

        return self.toConfigurationValidationProblems(validationItems, "ranger-env")

    def validateRangerAdminConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        ranger_site = properties
        validationItems = []
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        if 'RANGER' in servicesList and 'policymgr_external_url' in ranger_site:
            policymgr_mgr_url = ranger_site['policymgr_external_url']
            if policymgr_mgr_url.endswith('/'):
                validationItems.append({'config-name': 'policymgr_external_url',
                                        'item': self.getWarnItem(
                                            'Ranger External URL should not contain trailing slash "/"')})
        return self.toConfigurationValidationProblems(validationItems, 'admin-properties')

    def validateRangerTagsyncConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        ranger_tagsync_properties = properties
        validationItems = []
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

        has_atlas = False
        if "RANGER" in servicesList:
            has_atlas = not "ATLAS" in servicesList

            if has_atlas and 'ranger.tagsync.source.atlas' in ranger_tagsync_properties and \
                            ranger_tagsync_properties['ranger.tagsync.source.atlas'].lower() == 'true':
                validationItems.append({"config-name": "ranger.tagsync.source.atlas",
                                        "item": self.getWarnItem("Need to Install ATLAS service to set ranger.tagsync.source.atlas as true.")})

        return self.toConfigurationValidationProblems(validationItems, "ranger-tagsync-site")


    def validateHiveServer2Configurations(self, properties, recommendedDefaults, configurations, services, hosts):
        hive_server2 = properties
        validationItems = []
        # Adding Ranger Plugin logic here
        ranger_plugin_properties = getSiteProperties(configurations, "ranger-hive-plugin-properties")
        hive_env_properties = getSiteProperties(configurations, "hive-env")
        ranger_plugin_enabled = 'hive_security_authorization' in hive_env_properties and hive_env_properties[
                                                                                             'hive_security_authorization'].lower() == 'ranger'
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

        ##Add stack validations only if Ranger is enabled.
        if ("RANGER" in servicesList):
            ##Add stack validations for  Ranger plugin enabled.
            if ranger_plugin_enabled:
                prop_name = 'hive.security.authorization.manager'
                prop_val = "org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory"
                if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
                    validationItems.append({"config-name": prop_name,
                                            "item": self.getWarnItem(
                                                "If Ranger Hive Plugin is enabled." \
                                                " {0} under hiveserver2-site needs to be set to {1}".format(prop_name,
                                                                                                            prop_val))})
                prop_name = 'hive.security.authenticator.manager'
                prop_val = "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator"
                if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
                    validationItems.append({"config-name": prop_name,
                                            "item": self.getWarnItem(
                                                "If Ranger Hive Plugin is enabled." \
                                                " {0} under hiveserver2-site needs to be set to {1}".format(prop_name,
                                                                                                            prop_val))})
                prop_name = 'hive.security.authorization.enabled'
                prop_val = 'true'
                if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
                    validationItems.append({"config-name": prop_name,
                                            "item": self.getWarnItem(
                                                "If Ranger Hive Plugin is enabled." \
                                                " {0} under hiveserver2-site needs to be set to {1}".format(prop_name,
                                                                                                            prop_val))})
                prop_name = 'hive.conf.restricted.list'
                prop_vals = 'hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager'.split(
                    ',')
                current_vals = []
                missing_vals = []
                if hive_server2 and prop_name in hive_server2:
                    current_vals = hive_server2[prop_name].split(',')
                    current_vals = [x.strip() for x in current_vals]

                for val in prop_vals:
                    if not val in current_vals:
                        missing_vals.append(val)

                if missing_vals:
                    validationItems.append({"config-name": prop_name,
                                            "item": self.getWarnItem("If Ranger Hive Plugin is enabled." \
                                                                     " {0} under hiveserver2-site needs to contain missing value {1}".format(
                                                prop_name, ','.join(missing_vals)))})
            ##Add stack validations for  Ranger plugin disabled.
            elif not ranger_plugin_enabled:
                prop_name = 'hive.security.authorization.manager'
                prop_val = "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory"
                if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
                    validationItems.append({"config-name": prop_name,
                                            "item": self.getWarnItem(
                                                "If Ranger Hive Plugin is disabled." \
                                                " {0} needs to be set to {1}".format(prop_name, prop_val))})
                prop_name = 'hive.security.authenticator.manager'
                prop_val = "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator"
                if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
                    validationItems.append({"config-name": prop_name,
                                            "item": self.getWarnItem(
                                                "If Ranger Hive Plugin is disabled." \
                                                " {0} needs to be set to {1}".format(prop_name, prop_val))})
        return self.toConfigurationValidationProblems(validationItems, "hiveserver2-site")

    def validateWebhcatConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = [{"config-name": 'templeton.hadoop.queue.name',
                            "item": self.validatorYarnQueue(properties, recommendedDefaults,
                                                            'templeton.hadoop.queue.name', services)}]
        return self.toConfigurationValidationProblems(validationItems, "webhcat-site")

    def validateHiveConfigurationsEnv(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []
        hive_env = properties
        hive_site = getSiteProperties(configurations, "hive-site")
        if "hive_security_authorization" in hive_env and \
                        str(hive_env["hive_security_authorization"]).lower() == "none" \
                and str(hive_site["hive.security.authorization.enabled"]).lower() == "true":
            authorization_item = self.getErrorItem("hive_security_authorization should not be None "
                                                   "if hive.security.authorization.enabled is set")
            validationItems.append({"config-name": "hive_security_authorization", "item": authorization_item})
        if "hive_security_authorization" in hive_env and \
                        str(hive_env["hive_security_authorization"]).lower() == "ranger":
            # ranger-hive-plugin must be enabled in ranger-env
            ranger_env = getServicesSiteProperties(services, 'ranger-env')
            if not ranger_env or not 'ranger-hive-plugin-enabled' in ranger_env or \
                            ranger_env['ranger-hive-plugin-enabled'].lower() != 'yes':
                validationItems.append({"config-name": 'hive_security_authorization',
                                        "item": self.getWarnItem(
                                            "ranger-env/ranger-hive-plugin-enabled must be enabled when hive_security_authorization is set to Ranger")})
        return self.toConfigurationValidationProblems(validationItems, "hive-env")

    def validateHiveConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        hive_site = properties
        validationItems = []
        stripe_size_values = [8388608, 16777216, 33554432, 67108864, 134217728, 268435456]
        stripe_size_property = "hive.exec.orc.default.stripe.size"
        if stripe_size_property in properties and \
                        int(properties[stripe_size_property]) not in stripe_size_values:
            validationItems.append({"config-name": stripe_size_property,
                                    "item": self.getWarnItem("Correct values are {0}".format(stripe_size_values))
                                    }
                                   )
        authentication_property = "hive.server2.authentication"
        ldap_baseDN_property = "hive.server2.authentication.ldap.baseDN"
        ldap_domain_property = "hive.server2.authentication.ldap.Domain"
        if authentication_property in properties and properties[authentication_property].lower() == "ldap" \
                and not (ldap_baseDN_property in properties or ldap_domain_property in properties):
            validationItems.append({"config-name": authentication_property, "item":
                self.getWarnItem("According to LDAP value for " + authentication_property + ", you should add " +
                                 ldap_domain_property + " property, if you are using AD, if not, then " + ldap_baseDN_property + "!")})

        configurationValidationProblems = self.toConfigurationValidationProblems(validationItems, "hive-site")
        return configurationValidationProblems

    def validateHbaseEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        hbase_site = getSiteProperties(configurations, "hbase-site")
        validationItems = [ {"config-name": 'hbase_regionserver_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'hbase_regionserver_heapsize')},
                            {"config-name": 'hbase_master_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'hbase_master_heapsize')},
                            {"config-name": "hbase_user", "item": self.validatorEqualsPropertyItem(properties, "hbase_user", hbase_site, "hbase.superuser")} ]
        # Adding Ranger Plugin logic here
        ranger_plugin_properties = getSiteProperties(configurations, "ranger-hbase-plugin-properties")
        ranger_plugin_enabled = ranger_plugin_properties['ranger-hbase-plugin-enabled'] if ranger_plugin_properties else 'No'
        prop_name = 'hbase.security.authorization'
        prop_val = "true"
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()):
            if hbase_site[prop_name] != prop_val:
                validationItems.append({"config-name": prop_name,
                                        "item": self.getWarnItem("If Ranger HBase Plugin is enabled." \
                                            "{0} needs to be set to {1}".format(prop_name, prop_val))})
            prop_name = "hbase.coprocessor.master.classes"
            prop_val = "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor"
            exclude_val = "org.apache.hadoop.hbase.security.access.AccessController"
            if (prop_val in hbase_site[prop_name] and exclude_val not in hbase_site[prop_name]):
                pass
            else:
                validationItems.append({"config-name": prop_name,"item": self.getWarnItem("If Ranger HBase Plugin is enabled." \
                                            " {0} needs to contain {1} instead of {2}".format(prop_name, prop_val, exclude_val))})
            prop_name = "hbase.coprocessor.region.classes"
            prop_val = "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor"
            if (prop_val in hbase_site[prop_name] and exclude_val not in hbase_site[prop_name]):
                pass
            else:
                validationItems.append({"config-name": prop_name,
                                        "item": self.getWarnItem("If Ranger HBase Plugin is enabled." \
                                            " {0} needs to contain {1} instead of {2}".format(prop_name, prop_val, exclude_val))})

        return self.toConfigurationValidationProblems(validationItems, "hbase-env")

    def validateHBASERangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []
        ranger_plugin_properties = getSiteProperties(configurations, "ranger-hbase-plugin-properties")
        ranger_plugin_enabled = ranger_plugin_properties['ranger-hbase-plugin-enabled'] if ranger_plugin_properties else 'No'
        if ranger_plugin_enabled.lower() == 'yes':
            # ranger-hdfs-plugin must be enabled in ranger-env
            ranger_env = getServicesSiteProperties(services, 'ranger-env')
            if not ranger_env or not 'ranger-hbase-plugin-enabled' in ranger_env or ranger_env['ranger-hbase-plugin-enabled'].lower() != 'yes':
                validationItems.append({"config-name": 'ranger-hbase-plugin-enabled',
                                        "item": self.getWarnItem(
                                            "ranger-hbase-plugin-properties/ranger-hbase-plugin-enabled must correspond ranger-env/ranger-hbase-plugin-enabled")})
        return self.toConfigurationValidationProblems(validationItems, "ranger-hbase-plugin-properties")

    def validateHDFSConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        clusterEnv = getSiteProperties(configurations, "cluster-env")
        validationItems = [{"config-name": 'dfs.datanode.du.reserved', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'dfs.datanode.du.reserved')},
                           {"config-name": 'dfs.datanode.data.dir', "item": self.validatorOneDataDirPerPartition(properties, 'dfs.datanode.data.dir', services, hosts, clusterEnv)}]

        # We can not access property hadoop.security.authentication from the
        # other config (core-site). That's why we are using another heuristics here
        hdfs_site = properties
        core_site = getSiteProperties(configurations, "core-site")

        dfs_encrypt_data_transfer = 'dfs.encrypt.data.transfer'  # Hadoop Wire encryption
        try:
            wire_encryption_enabled = hdfs_site[dfs_encrypt_data_transfer] == "true"
        except KeyError:
            wire_encryption_enabled = False

        HTTP_ONLY = 'HTTP_ONLY'
        HTTPS_ONLY = 'HTTPS_ONLY'
        HTTP_AND_HTTPS = 'HTTP_AND_HTTPS'

        VALID_HTTP_POLICY_VALUES = [HTTP_ONLY, HTTPS_ONLY, HTTP_AND_HTTPS]
        VALID_TRANSFER_PROTECTION_VALUES = ['authentication', 'integrity', 'privacy']

        validationItems = []
        address_properties = [
            # "dfs.datanode.address",
            # "dfs.datanode.http.address",
            # "dfs.datanode.https.address",
            # "dfs.datanode.ipc.address",
            # "dfs.journalnode.http-address",
            # "dfs.journalnode.https-address",
            # "dfs.namenode.rpc-address",
            # "dfs.namenode.secondary.http-address",
            "dfs.namenode.http-address",
            "dfs.namenode.https-address",
        ]
        # Validating *address properties for correct values

        for address_property in address_properties:
            if address_property in hdfs_site:
                value = hdfs_site[address_property]
                if not is_valid_host_port_authority(value):
                    validationItems.append({"config-name": address_property, "item":
                        self.getErrorItem(address_property + " does not contain a valid host:port authority: " + value)})

        # Adding Ranger Plugin logic here
        ranger_plugin_properties = getSiteProperties(configurations, "ranger-hdfs-plugin-properties")
        ranger_plugin_enabled = ranger_plugin_properties['ranger-hdfs-plugin-enabled'] if ranger_plugin_properties else 'No'

        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()):
            if 'dfs.permissions.enabled' in hdfs_site and hdfs_site['dfs.permissions.enabled'] != 'true':
                validationItems.append({"config-name": 'dfs.permissions.enabled',
                                        "item": self.getWarnItem("dfs.permissions.enabled needs to be set to true if Ranger HDFS Plugin is enabled.")})

        if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()):
            if 'dfs.namenode.inode.attributes.provider.class' not in hdfs_site or \
                hdfs_site['dfs.namenode.inode.attributes.provider.class'].lower() != 'org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer'.lower():
                validationItems.append({"config-name": 'dfs.namenode.inode.attributes.provider.class',
                                        "item": self.getWarnItem(
                                            "dfs.namenode.inode.attributes.provider.class needs to be set to 'org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer' if Ranger HDFS Plugin is enabled.")})


        if (not wire_encryption_enabled and  # If wire encryption is enabled at Hadoop, it disables all our checks
                    'hadoop.security.authentication' in core_site and
                    core_site['hadoop.security.authentication'] == 'kerberos' and
                    'hadoop.security.authorization' in core_site and
                    core_site['hadoop.security.authorization'] == 'true'):
            # security is enabled

            dfs_http_policy = 'dfs.http.policy'
            dfs_datanode_address = 'dfs.datanode.address'
            datanode_http_address = 'dfs.datanode.http.address'
            datanode_https_address = 'dfs.datanode.https.address'
            data_transfer_protection = 'dfs.data.transfer.protection'

            try:  # Params may be absent
                privileged_dfs_dn_port = isSecurePort(getPort(hdfs_site[dfs_datanode_address]))
            except KeyError:
                privileged_dfs_dn_port = False
            try:
                privileged_dfs_http_port = isSecurePort(getPort(hdfs_site[datanode_http_address]))
            except KeyError:
                privileged_dfs_http_port = False
            try:
                privileged_dfs_https_port = isSecurePort(getPort(hdfs_site[datanode_https_address]))
            except KeyError:
                privileged_dfs_https_port = False
            try:
                dfs_http_policy_value = hdfs_site[dfs_http_policy]
            except KeyError:
                dfs_http_policy_value = HTTP_ONLY  # Default
            try:
                data_transfer_protection_value = hdfs_site[data_transfer_protection]
            except KeyError:
                data_transfer_protection_value = None

            if dfs_http_policy_value not in VALID_HTTP_POLICY_VALUES:
                validationItems.append({"config-name": dfs_http_policy,
                                        "item": self.getWarnItem(
                                            "Invalid property value: {0}. Valid values are {1}".format(
                                                dfs_http_policy_value, VALID_HTTP_POLICY_VALUES))})

            # determine whether we use secure ports
            address_properties_with_warnings = []
            if dfs_http_policy_value == HTTPS_ONLY:
                if not privileged_dfs_dn_port and (
                    privileged_dfs_https_port or datanode_https_address not in hdfs_site):
                    important_properties = [dfs_datanode_address, datanode_https_address]
                    message = "You set up datanode to use some non-secure ports. " \
                              "If you want to run Datanode under non-root user in a secure cluster, " \
                              "you should set all these properties {2} " \
                              "to use non-secure ports (if property {3} does not exist, " \
                              "just add it). You may also set up property {4} ('{5}' is a good default value). " \
                              "Also, set up WebHDFS with SSL as " \
                              "described in manual in order to be able to " \
                              "use HTTPS.".format(dfs_http_policy, dfs_http_policy_value, important_properties,
                                                  datanode_https_address, data_transfer_protection,
                                                  VALID_TRANSFER_PROTECTION_VALUES[0])
                    address_properties_with_warnings.extend(important_properties)
            else:  # dfs_http_policy_value == HTTP_AND_HTTPS or HTTP_ONLY
                # We don't enforce datanode_https_address to use privileged ports here
                any_nonprivileged_ports_are_in_use = not privileged_dfs_dn_port or not privileged_dfs_http_port
                if any_nonprivileged_ports_are_in_use:
                    important_properties = [dfs_datanode_address, datanode_http_address]
                    message = "You have set up datanode to use some non-secure ports, but {0} is set to {1}. " \
                              "In a secure cluster, Datanode forbids using non-secure ports " \
                              "if {0} is not set to {3}. " \
                              "Please make sure that properties {2} use secure ports.".format(
                        dfs_http_policy, dfs_http_policy_value, important_properties, HTTPS_ONLY)
                    address_properties_with_warnings.extend(important_properties)

            # Generate port-related warnings if any
            for prop in address_properties_with_warnings:
                validationItems.append({"config-name": prop,
                                        "item": self.getWarnItem(message)})

            # Check if it is appropriate to use dfs.data.transfer.protection
            if data_transfer_protection_value is not None:
                if dfs_http_policy_value in [HTTP_ONLY, HTTP_AND_HTTPS]:
                    validationItems.append({"config-name": data_transfer_protection,
                                            "item": self.getWarnItem(
                                                "{0} property can not be used when {1} is set to any "
                                                "value other then {2}. Tip: When {1} property is not defined, it defaults to {3}".format(
                                                    data_transfer_protection, dfs_http_policy, HTTPS_ONLY, HTTP_ONLY))})
                elif not data_transfer_protection_value in VALID_TRANSFER_PROTECTION_VALUES:
                    validationItems.append({"config-name": data_transfer_protection,
                                            "item": self.getWarnItem(
                                                "Invalid property value: {0}. Valid values are {1}.".format(
                                                    data_transfer_protection_value, VALID_TRANSFER_PROTECTION_VALUES))})

        return self.toConfigurationValidationProblems(validationItems, "hdfs-site")

    def validateHDFSConfigurationsEnv(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = [ {"config-name": 'namenode_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_heapsize')},
                            {"config-name": 'namenode_opt_newsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_opt_newsize')},
                            {"config-name": 'namenode_opt_maxnewsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_opt_maxnewsize')}]
        return self.toConfigurationValidationProblems(validationItems, "hadoop-env")

    def validateHDFSRangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []
        ranger_plugin_properties = getSiteProperties(configurations, "ranger-hdfs-plugin-properties")
        ranger_plugin_enabled = ranger_plugin_properties['ranger-hdfs-plugin-enabled'] if ranger_plugin_properties else 'No'
        if (ranger_plugin_enabled.lower() == 'yes'):
            # ranger-hdfs-plugin must be enabled in ranger-env
            ranger_env = getServicesSiteProperties(services, 'ranger-env')
            if not ranger_env or not 'ranger-hdfs-plugin-enabled' in ranger_env or ranger_env['ranger-hdfs-plugin-enabled'].lower() != 'yes':
                validationItems.append({"config-name": 'ranger-hdfs-plugin-enabled',
                                        "item": self.getWarnItem(
                                            "ranger-hdfs-plugin-properties/ranger-hdfs-plugin-enabled must correspond ranger-env/ranger-hdfs-plugin-enabled")})
        return self.toConfigurationValidationProblems(validationItems, "ranger-hdfs-plugin-properties")

    def validatorOneDataDirPerPartition(self, properties, propertyName, services, hosts, clusterEnv):
        if not propertyName in properties:
            return self.getErrorItem("Value should be set")
        dirs = properties[propertyName]

        if not (clusterEnv and "one_dir_per_partition" in clusterEnv and clusterEnv["one_dir_per_partition"].lower() == "true"):
            return None

        dataNodeHosts = self.getDataNodeHosts(services, hosts)

        warnings = set()
        for host in dataNodeHosts:
            hostName = host["Hosts"]["host_name"]

            mountPoints = []
            for diskInfo in host["Hosts"]["disk_info"]:
                mountPoints.append(diskInfo["mountpoint"])

            if get_mounts_with_multiple_data_dirs(mountPoints, dirs):
                # A detailed message can be too long on large clusters:
                # warnings.append("Host: " + hostName + "; Mount: " + mountPoint + "; Data directories: " + ", ".join(dirList))
                warnings.add(hostName)
                break;

        if len(warnings) > 0:
            return self.getWarnItem("cluster-env/one_dir_per_partition is enabled but there are multiple data directories on the same mount. Affected hosts: {0}".format(", ".join(sorted(warnings))))

        return None

    """
    Returns the list of Data Node hosts.
    """
    def getDataNodeHosts(self, services, hosts):
        if len(hosts["items"]) > 0:
            dataNodeHosts = self.getHostsWithComponent("HDFS", "DATANODE", services, hosts)
            if dataNodeHosts is not None:
                return dataNodeHosts
        return []

    def getMastersWithMultipleInstances(self):
        return ['ZOOKEEPER_SERVER', 'HBASE_MASTER']

    def getNotValuableComponents(self):
        return ['JOURNALNODE', 'ZKFC', 'GANGLIA_MONITOR', 'APP_TIMELINE_SERVER']

    def getNotPreferableOnServerComponents(self):
        return ['GANGLIA_SERVER', 'METRICS_COLLECTOR']

    def getCardinalitiesDict(self):
        return {
            'ZOOKEEPER_SERVER': {"min": 3},
            'HBASE_MASTER': {"min": 1},
        }

    def getComponentLayoutSchemes(self):
        return {
            'NAMENODE': {"else": 0},
            'SECONDARY_NAMENODE': {"else": 1},
            'HBASE_MASTER': {6: 0, 31: 2, "else": 3},
            'APP_TIMELINE_SERVER': {31: 1, "else": 2},

            'HISTORYSERVER': {31: 1, "else": 2},
            'RESOURCEMANAGER': {31: 1, "else": 2},

            'OOZIE_SERVER': {6: 1, 31: 2, "else": 3},

            'HIVE_SERVER': {6: 1, 31: 2, "else": 4},
            'HIVE_METASTORE': {6: 1, 31: 2, "else": 4},
            'WEBHCAT_SERVER': {6: 1, 31: 2, "else": 4},
            'METRICS_COLLECTOR': {3: 2, 6: 2, 31: 3, "else": 5},
        }

    def get_system_min_uid(self):
        login_defs = '/etc/login.defs'
        uid_min_tag = 'UID_MIN'
        comment_tag = '#'
        uid_min = uid_default = '1000'
        uid = None

        if os.path.exists(login_defs):
            with open(login_defs, 'r') as f:
                data = f.read().split('\n')
                # look for uid_min_tag in file
                uid = filter(lambda x: uid_min_tag in x, data)
                # filter all lines, where uid_min_tag was found in comments
                uid = filter(lambda x: x.find(comment_tag) > x.find(uid_min_tag) or x.find(comment_tag) == -1, uid)

            if uid is not None and len(uid) > 0:
                uid = uid[0]
                comment = uid.find(comment_tag)
                tag = uid.find(uid_min_tag)
                if comment == -1:
                    uid_tag = tag + len(uid_min_tag)
                    uid_min = uid[uid_tag:].strip()
                elif comment > tag:
                    uid_tag = tag + len(uid_min_tag)
                    uid_min = uid[uid_tag:comment].strip()

        # check result for value
        try:
            int(uid_min)
        except ValueError:
            return uid_default

        return uid_min

    def mergeValidators(self, parentValidators, childValidators):
        for service, configsDict in childValidators.iteritems():
            if service not in parentValidators:
                parentValidators[service] = {}
            parentValidators[service].update(configsDict)

    def checkSiteProperties(self, siteProperties, *propertyNames):
        """
        Check if properties defined in site properties.
        :param siteProperties: config properties dict
        :param *propertyNames: property names to validate
        :returns: True if all properties defined, in other cases returns False
        """
        if siteProperties is None:
            return False
        for name in propertyNames:
            if not (name in siteProperties):
                return False
        return True

    """
    Returns the dictionary of configs for 'capacity-scheduler'.
    """
    def getCapacitySchedulerProperties(self, services):
        capacity_scheduler_properties = dict()
        received_as_key_value_pair = True
        if "capacity-scheduler" in services['configurations']:
            if "capacity-scheduler" in services['configurations']["capacity-scheduler"]["properties"]:
                cap_sched_props_as_str = services['configurations']["capacity-scheduler"]["properties"]["capacity-scheduler"]
                if cap_sched_props_as_str:
                    cap_sched_props_as_str = str(cap_sched_props_as_str).split('\n')
                    if len(cap_sched_props_as_str) > 0 and cap_sched_props_as_str[0] != 'null':
                        # Received confgs as one "\n" separated string
                        for property in cap_sched_props_as_str:
                            key, sep, value = property.partition("=")
                            capacity_scheduler_properties[key] = value
                        Logger.info("'capacity-scheduler' configs is passed-in as a single '\\n' separated string. "
                                    "count(services['configurations']['capacity-scheduler']['properties']['capacity-scheduler']) = "
                                    "{0}".format(len(capacity_scheduler_properties)))
                        received_as_key_value_pair = False
                    else:
                        Logger.info("Passed-in services['configurations']['capacity-scheduler']['properties']['capacity-scheduler'] is 'null'.")
                else:
                    Logger.info("'capacity-schdeuler' configs not passed-in as single '\\n' string in "
                                "services['configurations']['capacity-scheduler']['properties']['capacity-scheduler'].")
            if not capacity_scheduler_properties:
                # Received configs as a dictionary (Generally on 1st invocation).
                capacity_scheduler_properties = services['configurations']["capacity-scheduler"]["properties"]
                Logger.info("'capacity-scheduler' configs is passed-in as a dictionary. "
                            "count(services['configurations']['capacity-scheduler']['properties']) = {0}".format(len(capacity_scheduler_properties)))
        else:
            Logger.error("Couldn't retrieve 'capacity-scheduler' from services.")

        Logger.info("Retrieved 'capacity-scheduler' received as dictionary : '{0}'. configs : {1}" \
                    .format(received_as_key_value_pair, capacity_scheduler_properties.items()))
        return capacity_scheduler_properties, received_as_key_value_pair

    """
    Gets all YARN leaf queues.
    """
    def getAllYarnLeafQueues(self, capacitySchedulerProperties):
        config_list = capacitySchedulerProperties.keys()
        yarn_queues = None
        leafQueueNames = set()
        if 'yarn.scheduler.capacity.root.queues' in config_list:
            yarn_queues = capacitySchedulerProperties.get('yarn.scheduler.capacity.root.queues')

        if yarn_queues:
            toProcessQueues = yarn_queues.split(",")
            while len(toProcessQueues) > 0:
                queue = toProcessQueues.pop()
                queueKey = "yarn.scheduler.capacity.root." + queue + ".queues"
                if queueKey in capacitySchedulerProperties:
                    # If parent queue, add children
                    subQueues = capacitySchedulerProperties[queueKey].split(",")
                    for subQueue in subQueues:
                        toProcessQueues.append(queue + "." + subQueue)
                else:
                    # Leaf queues
                    # We only take the leaf queue name instead of the complete path, as leaf queue names are unique in YARN.
                    # Eg: If YARN queues are like :
                    #     (1). 'yarn.scheduler.capacity.root.a1.b1.c1.d1',
                    #     (2). 'yarn.scheduler.capacity.root.a1.b1.c2',
                    #     (3). 'yarn.scheduler.capacity.root.default,
                    # Added leaf queues names are as : d1, c2 and default for the 3 leaf queues.
                    leafQueuePathSplits = queue.split(".")
                    if leafQueuePathSplits > 0:
                        leafQueueName = leafQueuePathSplits[-1]
                        leafQueueNames.add(leafQueueName)
        return leafQueueNames

    def get_service_component_meta(self, service, component, services):
        """
        Function retrieve service component meta information as dict from services.json
        If no service or component found, would be returned empty dict

        Return value example:
            "advertise_version" : true,
            "bulk_commands_display_name" : "",
            "bulk_commands_master_component_name" : "",
            "cardinality" : "1+",
            "component_category" : "CLIENT",
            "component_name" : "HBASE_CLIENT",
            "custom_commands" : [ ],
            "decommission_allowed" : false,
            "display_name" : "HBase Client",
            "has_bulk_commands_definition" : false,
            "is_client" : true,
            "is_master" : false,
            "reassign_allowed" : false,
            "recovery_enabled" : false,
            "service_name" : "HBASE",
            "stack_name" : "HDP",
            "stack_version" : "2.5",
            "hostnames" : [ "host1", "host2" ]

        :type service str
        :type component str
        :type services dict
        :rtype dict
        """
        __stack_services = "StackServices"
        __stack_service_components = "StackServiceComponents"

        if not services:
            return {}

        service_meta = [item for item in services["services"] if item[__stack_services]["service_name"] == service]
        if len(service_meta) == 0:
            return {}

        service_meta = service_meta[0]
        component_meta = [item for item in service_meta["components"] if item[__stack_service_components]["component_name"] == component]

        if len(component_meta) == 0:
            return {}

        return component_meta[0][__stack_service_components]

    def is_secured_cluster(self, services):
        """
        Detects if cluster is secured or not
        :type services dict
        :rtype bool
        """
        return services and "cluster-env" in services["configurations"] and \
               "security_enabled" in services["configurations"]["cluster-env"]["properties"] and \
               services["configurations"]["cluster-env"]["properties"]["security_enabled"].lower() == "true"

    def get_services_list(self, services):
        """
        Returns available services as list

        :type services dict
        :rtype list
        """
        if not services:
            return []

        return [service["StackServices"]["service_name"] for service in services["services"]]

    def get_components_list(self, service, services):
        """
        Return list of components for specific service
        :type service str
        :type services dict
        :rtype list
        """
        __stack_services = "StackServices"
        __stack_service_components = "StackServiceComponents"

        if not services:
            return []

        service_meta = [item for item in services["services"] if item[__stack_services]["service_name"] == service]
        if len(service_meta) == 0:
            return []

        service_meta = service_meta[0]
        return [item[__stack_service_components]["component_name"] for item in service_meta["components"]]


    def getOldValue(self, services, configType, propertyName):
        if services:
            if 'changed-configurations' in services.keys():
                changedConfigs = services["changed-configurations"]
                for changedConfig in changedConfigs:
                    if changedConfig["type"] == configType and changedConfig["name"]== propertyName and "old_value" in changedConfig:
                        return changedConfig["old_value"]
        return None


    def getDBDriver(self, databaseType):
        driverDict = {
            'NEW MYSQL DATABASE': 'com.mysql.jdbc.Driver',
            'NEW DERBY DATABASE': 'org.apache.derby.jdbc.EmbeddedDriver',
            'EXISTING MYSQL DATABASE': 'com.mysql.jdbc.Driver',
            'EXISTING MYSQL / MARIADB DATABASE': 'com.mysql.jdbc.Driver',
            'EXISTING POSTGRESQL DATABASE': 'org.postgresql.Driver',
            'EXISTING ORACLE DATABASE': 'oracle.jdbc.driver.OracleDriver',
            'EXISTING SQL ANYWHERE DATABASE': 'sap.jdbc4.sqlanywhere.IDriver'
        }
        return driverDict.get(databaseType.upper())

    def getDBConnectionString(self, databaseType):
        driverDict = {
            'NEW MYSQL DATABASE': 'jdbc:mysql://{0}/{1}?createDatabaseIfNotExist=true',
            'NEW DERBY DATABASE': 'jdbc:derby:${{oozie.data.dir}}/${{oozie.db.schema.name}}-db;create=true',
            'EXISTING MYSQL DATABASE': 'jdbc:mysql://{0}/{1}',
            'EXISTING MYSQL / MARIADB DATABASE': 'jdbc:mysql://{0}/{1}',
            'EXISTING POSTGRESQL DATABASE': 'jdbc:postgresql://{0}:5432/{1}',
            'EXISTING ORACLE DATABASE': 'jdbc:oracle:thin:@//{0}:1521/{1}',
            'EXISTING SQL ANYWHERE DATABASE': 'jdbc:sqlanywhere:host={0};database={1}'
        }
        return driverDict.get(databaseType.upper())

    def getProtocol(self, databaseType):
        first_parts_of_connection_string = {
            'NEW MYSQL DATABASE': 'jdbc:mysql',
            'NEW DERBY DATABASE': 'jdbc:derby',
            'EXISTING MYSQL DATABASE': 'jdbc:mysql',
            'EXISTING MYSQL / MARIADB DATABASE': 'jdbc:mysql',
            'EXISTING POSTGRESQL DATABASE': 'jdbc:postgresql',
            'EXISTING ORACLE DATABASE': 'jdbc:oracle',
            'EXISTING SQL ANYWHERE DATABASE': 'jdbc:sqlanywhere'
        }
        return first_parts_of_connection_string.get(databaseType.upper())

    def getDBTypeAlias(self, databaseType):
        driverDict = {
            'NEW MYSQL DATABASE': 'mysql',
            'NEW DERBY DATABASE': 'derby',
            'EXISTING MYSQL / MARIADB DATABASE': 'mysql',
            'EXISTING MYSQL DATABASE': 'mysql',
            'EXISTING POSTGRESQL DATABASE': 'postgres',
            'EXISTING ORACLE DATABASE': 'oracle',
            'EXISTING SQL ANYWHERE DATABASE': 'sqla'
        }
        return driverDict.get(databaseType.upper())

# Validation helper methods
def getSiteProperties(configurations, siteName):
    siteConfig = configurations.get(siteName)
    if siteConfig is None:
        return None
    return siteConfig.get("properties")

def getServicesSiteProperties(services, siteName):
    configurations = services.get("configurations")
    if not configurations:
        return None
    siteConfig = configurations.get(siteName)
    if siteConfig is None:
        return None
    return siteConfig.get("properties")

def to_number(s):
    try:
        return int(re.sub("\D", "", s))
    except ValueError:
        return None

def checkXmxValueFormat(value):
    p = re.compile('-Xmx(\d+)(b|k|m|g|p|t|B|K|M|G|P|T)?')
    matches = p.findall(value)
    return len(matches) == 1

def getXmxSize(value):
    p = re.compile("-Xmx(\d+)(.?)")
    result = p.findall(value)[0]
    if len(result) > 1:
        # result[1] - is a space or size formatter (b|k|m|g etc)
        return result[0] + result[1].lower()
    return result[0]

def formatXmxSizeToBytes(value):
    value = value.lower()
    if len(value) == 0:
        return 0
    modifier = value[-1]

    if modifier == ' ' or modifier in "0123456789":
        modifier = 'b'
    m = {
        modifier == 'b': 1,
        modifier == 'k': 1024,
        modifier == 'm': 1024 * 1024,
        modifier == 'g': 1024 * 1024 * 1024,
        modifier == 't': 1024 * 1024 * 1024 * 1024,
        modifier == 'p': 1024 * 1024 * 1024 * 1024 * 1024
    }[1]
    return to_number(value) * m

def getPort(address):
    """
    Extracts port from the address like 0.0.0.0:1019
    """
    if address is None:
        return None
    m = re.search(r'(?:http(?:s)?://)?([\w\d.]*):(\d{1,5})', address)
    if m is not None:
        return int(m.group(2))
    else:
        return None

def isSecurePort(port):
    """
    Returns True if port is root-owned at *nix systems
    """
    if port is not None:
        return port < 1024
    else:
        return False

def getMountPointForDir(dir, mountPoints):
    """
    :param dir: Directory to check, even if it doesn't exist.
    :return: Returns the closest mount point as a string for the directory.
    if the "dir" variable is None, will return None.
    If the directory does not exist, will return "/".
    """
    bestMountFound = None
    if dir:
        dir = re.sub("^file://", "", dir, count=1).strip().lower()

        # If the path is "/hadoop/hdfs/data", then possible matches for mounts could be
        # "/", "/hadoop/hdfs", and "/hadoop/hdfs/data".
        # So take the one with the greatest number of segments.
        for mountPoint in mountPoints:
            # Ensure that the mount path and the dir path ends with "/"
            # The mount point "/hadoop" should not match with the path "/hadoop1"
            if os.path.join(dir, "").startswith(os.path.join(mountPoint, "")):
                if bestMountFound is None:
                    bestMountFound = mountPoint
                elif os.path.join(bestMountFound, "").count(os.path.sep) < os.path.join(mountPoint, "").count(os.path.sep):
                    bestMountFound = mountPoint

    return bestMountFound

def getHeapsizeProperties():
    return { "NAMENODE": [{"config-name": "hadoop-env",
                           "property": "namenode_heapsize",
                           "default": "1024m"}],
             "SECONDARY_NAMENODE": [{"config-name": "hadoop-env",
                                     "property": "namenode_heapsize",
                                     "default": "1024m"}],
             "DATANODE": [{"config-name": "hadoop-env",
                           "property": "dtnode_heapsize",
                           "default": "1024m"}],
             "REGIONSERVER": [{"config-name": "hbase-env",
                               "property": "hbase_regionserver_heapsize",
                               "default": "1024m"}],
             "HBASE_MASTER": [{"config-name": "hbase-env",
                               "property": "hbase_master_heapsize",
                               "default": "1024m"}],
             "HIVE_CLIENT": [{"config-name": "hive-env",
                              "property": "hive.client.heapsize",
                              "default": "1024"}],
             "HIVE_METASTORE": [{"config-name": "hive-env",
                                 "property": "hive.metastore.heapsize",
                                 "default": "1024"}],
             "HIVE_SERVER": [{"config-name": "hive-env",
                              "property": "hive.heapsize",
                              "default": "1024"}],
             "HISTORYSERVER": [{"config-name": "mapred-env",
                                "property": "jobhistory_heapsize",
                                "default": "1024m"}],
             "OOZIE_SERVER": [{"config-name": "oozie-env",
                               "property": "oozie_heapsize",
                               "default": "1024m"}],
             "RESOURCEMANAGER": [{"config-name": "yarn-env",
                                  "property": "resourcemanager_heapsize",
                                  "default": "1024m"}],
             "NODEMANAGER": [{"config-name": "yarn-env",
                              "property": "nodemanager_heapsize",
                              "default": "1024m"}],
             "APP_TIMELINE_SERVER": [{"config-name": "yarn-env",
                                      "property": "apptimelineserver_heapsize",
                                      "default": "1024m"}],
             "ZOOKEEPER_SERVER": [{"config-name": "zookeeper-env",
                                   "property": "zk_server_heapsize",
                                   "default": "1024m"}],
             "METRICS_COLLECTOR": [{"config-name": "ams-hbase-env",
                                    "property": "hbase_master_heapsize",
                                    "default": "1024"},
                                   {"config-name": "ams-hbase-env",
                                    "property": "hbase_regionserver_heapsize",
                                    "default": "1024"},
                                   {"config-name": "ams-env",
                                    "property": "metrics_collector_heapsize",
                                    "default": "512"}],
             "ATLAS_SERVER": [{"config-name": "atlas-env",
                               "property": "atlas_server_xmx",
                               "default": "2048"}],
             "LOGSEARCH_SERVER": [{"config-name": "logsearch-env",
                                   "property": "logsearch_app_max_memory",
                                   "default": "1024"}],
             "LOGSEARCH_LOGFEEDER": [{"config-name": "logfeeder-env",
                                      "property": "logfeeder_max_mem",
                                      "default": "512"}],
             "SPARK_JOBHISTORYSERVER": [{"config-name": "spark-env",
                                         "property": "spark_daemon_memory",
                                         "default": "1024"}],
             "SPARK2_JOBHISTORYSERVER": [{"config-name": "spark2-env",
                                          "property": "spark_daemon_memory",
                                          "default": "1024"}]
             }

def getMemorySizeRequired(components, configurations):
    totalMemoryRequired = 512*1024*1024 # 512Mb for OS needs
    for component in components:
        if component in getHeapsizeProperties().keys():
            heapSizeProperties = getHeapsizeProperties()[component]
            for heapSizeProperty in heapSizeProperties:
                try:
                    properties = configurations[heapSizeProperty["config-name"]]["properties"]
                    heapsize = properties[heapSizeProperty["property"]]
                except KeyError:
                    heapsize = heapSizeProperty["default"]

                # Assume Mb if no modifier
                if len(heapsize) > 1 and heapsize[-1] in '0123456789':
                    heapsize = str(heapsize) + "m"

                totalMemoryRequired += formatXmxSizeToBytes(heapsize)
        else:
            if component == "METRICS_MONITOR" or "CLIENT" in component:
                heapsize = '512m'
            else:
                heapsize = '1024m'
            totalMemoryRequired += formatXmxSizeToBytes(heapsize)
    return totalMemoryRequired

def round_to_n(mem_size, n=128):
    return int(round(mem_size / float(n))) * int(n)
def is_valid_host_port_authority(target):
  has_scheme = "://" in target
  if not has_scheme:
    target = "dummyscheme://"+target
  try:
    result = urlparse(target)
    if result.hostname is not None and result.port is not None:
      return True
  except ValueError:
    pass
  return False