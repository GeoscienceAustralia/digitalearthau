#!/usr/bin/env python3

import datetime
import os
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path
import shutil
import string
import logging
import yaml

MODULE_DIR = '/g/data/v10/public/modules'
MINICONDA_URL = 'https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh'

LOG = logging.getLogger('environment_module_builder')

def pre_check(config):
    LOG.debug('Performing pre-check before installing module')
    if "PYTHONPATH" in os.environ:
        raise Exception("The PYTHONPATH environment variable must NOT be set when creating modules.")

    module_path = Path(config['variables']['module_path'])
    if module_path.exists():
        raise Exception(f"The destination path {module_path} already exists, "
                        f"please remove it and try again.")


#    "LOADEDMODULES=pbs"
#    "MODULEPATH=/g/data/v10/public/modules/modulefiles:/apps/.mf:/opt/Modules/modulefiles:/apps/Modules/modulefiles:"

def prep(config_path):
    LOG.debug('Preparing environment variables')
    # Write files as group and world readable
    os.umask(0o22)
    os.chdir(config_path.parent)
    os.environ['LC_ALL'] = 'en_AU.utf8'
    os.environ['LANG'] = 'C.UTF-8'


def date(date_format="%Y%m%d") -> str:
    return datetime.datetime.now().strftime(date_format)


def make_output_dir(base_path) -> Path:
    today = date()

    output_dir = base_path / today

    while output_dir.exists():
        index = 1
        output_dir = base_path / f"{today}_{index}"

    output_dir.mkdir(exist_ok=False)
    return output_dir


def run(cmd: str):
    LOG.debug('Running command: %s', cmd)
    return subprocess.run(cmd, shell=True, check=True, stdout=sys.stdout, stderr=sys.stderr)


def install_conda(conda_conf, variables):
    fill_templates_from_variables(conda_conf, variables)
    destination_path = Path(conda_conf['dest'])
    LOG.debug('Installing miniconda to %s', destination_path)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        miniconda_installer = tmpdir / "miniconda.sh"
        LOG.debug('Downloading miniconda installer from "%s" to "%s"',
                  conda_conf['url'], miniconda_installer)
        urllib.request.urlretrieve(conda_conf['url'], miniconda_installer)
        miniconda_installer.chmod(0x755)

        run(f"{miniconda_installer} -b -p {destination_path}")

    conda = destination_path / "bin/conda"
    pip = destination_path / "bin/pip"

    # As of 9 March 2018, conda is broken on raijin without this
    run(f"{pip} install -U pyopenssl")

    run(f"{conda} update -n base -y conda")

    run(f"{conda} update --all -y")

#    run(f"{conda} config --prepend channels conda-forge --system")
    # update root env to the latest python and packages


def install_conda_packages(env_file, variables):
    LOG.debug('Installing conda packages from %s', env_file)
    # make sure no .local stuff interferes with the install
    os.environ['PYTHONNOUSERSITE'] = "1"

    env_name = variables['module_name']
    #run(f"{conda_bin_path} env update -n root -v --file {env_file}")
    conda_path = variables['conda_path']
    module_path = variables['module_path']

    run(f"{conda_path} env create -p {module_path} -v --file {env_file}")


def write_template(template_file, variables, output_file):
    LOG.debug('Filling template file %s to %s', template_file, output_file)
    LOG.debug('Ensuring parent dir %s exists', output_file.parent)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    template_contents = template_file.read_text()
    template = string.Template(template_contents)
    output_file.write_text(template.substitute(variables))


def fill_templates_from_variables(template_dict, variables):
    for key, val in template_dict.items():
        template_dict[key] = val.format(**variables)


def copy_files(copy_tasks, variables):
    for task in copy_tasks:
        fill_templates_from_variables(task, variables)
        src = Path(task['src'])
        dest = Path(task['dest'])

        LOG.debug('Copying %s to %s', src, dest)
        LOG.debug('Ensuring parent dir %s exists', dest.parent)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest)

        if 'chmod' in task:
            perms = int(task['chmod'], base=8)
            LOG.debug('Setting %s permissions to %s', dest, oct(perms))
            dest.chmod(perms)


def read_config(path):
    return yaml.safe_load(path.read_text())


def copy_and_fill_templates(template_tasks, variables):
    for task in template_tasks:
        fill_templates_from_variables(task, variables)

        src = Path(task['src'])
        dest = Path(task['dest'])
        write_template(src, variables, dest)

        if 'chmod' in task:
            perms = int(task['chmod'], base=8)
            dest.chmod(perms)


def include_templated_vars(config):
    fill_templates_from_variables(config['templated_variables'], config['variables'])
    config['variables'].update(config['templated_variables'])

    del config['templated_variables']


def fix_module_permissions(module_path):
    LOG.debug('Setting module "%s" to read-only', module_path)
    run(f'chmod -R a-w "{module_path}"')


def install_pip_packages(pip_conf, variables):
    fill_templates_from_variables(pip_conf, variables)
    pip = pip_conf['pip_cmd']
    dest = pip_conf['dest']
    requirements = pip_conf['requirements']
    LOG.debug('Installing pip packages from "%s" into directory "%s"',
              requirements, dest)
    run(f'{pip} install -v --no-deps --prefix {dest} --compile --requirement {requirements}')


def main(config_path):
    logging.basicConfig(level=logging.DEBUG)
    LOG.debug('Reading config file')
    config = read_config(config_path)
    config['variables']['module_version'] = date()
    include_templated_vars(config)

    variables = config['variables']

    pre_check(config)
    prep(config_path)

    if 'install_conda' in config:
        install_conda(config['install_conda'], variables)

    if 'install_conda_packages' in config:
        install_conda_packages(config['install_conda_packages'], variables)

    if 'install_pip_packages' in config:
        install_pip_packages(config['install_pip_packages'], variables)

    copy_files(config.get('copy_files', []), variables)
    copy_and_fill_templates(config.get('template_files', []), variables)
    fix_module_permissions(variables['module_path'])

if __name__ == '__main__':
    main(Path(sys.argv[1]))
