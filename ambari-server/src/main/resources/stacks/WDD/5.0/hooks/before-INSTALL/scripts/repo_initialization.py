import os
import sys
from ambari_commons.os_check import OSCheck
from resource_management import Execute, File, StaticFile, ExecuteScript
from resource_management.libraries.resources.repository import Repository
from resource_management.core.logger import Logger
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set.

# components_lits = repoName + postfix
_UBUNTU_REPO_COMPONENTS_POSTFIX = ["main"]
repo_auth_required = False

def _alter_repo(action, repo_string, repo_template):
  """
  @param action: "delete" or "create"
  @param repo_string: e.g. "[{\"baseUrl\":\"http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.0.6.0\",\"osType\":\"centos6\",\"repoId\":\"HDP-2.0._\",\"repoName\":\"HDP\",\"defaultBaseUrl\":\"http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.0.6.0\"}]"
  """
  repo_dicts = json.loads(repo_string)

  if not isinstance(repo_dicts, list):
    repo_dicts = [repo_dicts]

  if 0 == len(repo_dicts):
    Logger.info("Repository list is empty. Ambari may not be managing the repositories.")
  else:
    Logger.info("Initializing {0} repositories".format(str(len(repo_dicts))))

  for repo in repo_dicts:
    if not 'baseUrl' in repo:
      repo['baseUrl'] = None
    if not 'mirrorsList' in repo:
      repo['mirrorsList'] = None

    ubuntu_components = [ repo['repoName'] ] + _UBUNTU_REPO_COMPONENTS_POSTFIX

    Repository(repo['repoId'],
                 action = action,
                 base_url = repo['baseUrl'],
                 mirror_list = repo['mirrorsList'],
                 repo_file_name = repo['repoId'],
                 repo_template = repo_template,
                 components = ubuntu_components, # ubuntu specific
    )

def install_repos():
  import params
  if params.host_sys_prepped:
    Logger.info("Host sys prepped, no repos required")
    return

  template = params.repo_rhel_suse if OSCheck.is_suse_family() or OSCheck.is_redhat_family() else params.repo_ubuntu

  if repo_auth_required and OSCheck.is_redhat_family() and OSCheck.get_os_major_version() == "6":
      Logger.info("Need to patch redhat6 issue with urlgrabber http auth support...")
      patch_file = os.path.join(params.tmp_dir, "urlgrabber.patch")
      File(patch_file,
           content=StaticFile("urlgrabber.patch"),
           mode=0755
           )
      cmd = "patch /usr/lib/python2.6/site-packages/urlgrabber/grabber.py < {0}".format(patch_file)
      try:
          ExecuteScript("patch_urlgrabber_py",
                        code=cmd,
                        user="root")
      except:
          Logger.warning("Unexpected error: {0}".format(sys.exc_info()[0]))


  _alter_repo("create", params.repo_info, template)
  if params.service_repo_info:
    _alter_repo("create", params.service_repo_info, template)
