import os
import tempfile

from resource_management import Fail, Directory, File, DownloadSource, Execute
from resource_management.libraries.functions import format

def setup_java():
    """
    Installs jdk using specific params, that comes from ambari-server
    """
    import params

    java_exec = format("{java_home}/bin/java")

    if not os.path.isfile(java_exec):
        if not params.jdk_name:  # if custom jdk is used.
            raise Fail(format("Unable to access {java_exec}. Confirm you have copied jdk to this host."))

        jdk_curl_target = format("{tmp_dir}/{jdk_name}")
        java_dir = os.path.dirname(params.java_home)

        Directory(params.artifact_dir,
                  create_parents=True,
                  )

        File(jdk_curl_target,
             content=DownloadSource(format("{jdk_location}/{jdk_name}")),
             not_if=format("test -f {jdk_curl_target}")
             )

        tmp_java_dir = tempfile.mkdtemp(prefix="jdk_tmp_", dir=params.tmp_dir)

        try:
            if params.jdk_name.endswith(".bin"):
                chmod_cmd = ("chmod", "+x", jdk_curl_target)
                install_cmd = format(
                    "cd {tmp_java_dir} && echo A | {jdk_curl_target} -noregister && {sudo} cp -rp {tmp_java_dir}/* {java_dir}")
            elif params.jdk_name.endswith(".gz"):
                chmod_cmd = ("chmod", "a+x", java_dir)
                install_cmd = format(
                    "cd {tmp_java_dir} && tar -xf {jdk_curl_target} && {sudo} cp -rp {tmp_java_dir}/* {java_dir}")

            Directory(java_dir
                      )

            Execute(chmod_cmd,
                    sudo=True,
                    )

            Execute(install_cmd,
                    )

        finally:
            Directory(tmp_java_dir, action="delete")

        File(format("{java_home}/bin/java"),
             mode=0755,
             cd_access="a",
             )
        Execute(('chmod', '-R', '755', params.java_home),
                sudo=True,
                )
