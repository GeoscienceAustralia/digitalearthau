#!/usr/bin/env python3
"""
This program is used for deploying code modules onto the NCI.

It is configured by a YAML file, which specifies:
 - variables
 - files to copy with permissions
 - template files with their destination (eg, modulefiles)
 - (opt) Conda environment to create
 - (opt) Pip style requirements.txt to install to a directory

It requires python 3.6+ and pyyaml. To run it on raijin at the NCI:

  module load python3/3.6.2
  pip install --user pyyaml
  ./build_environment_module.py dea-env/modulespec.yaml

It used to be able to perform a miniconda installation, but that turned out to
be flaky, so we now maintain a central miniconda install, and create environments
as an where required. With the added benefit of keeping a central cache of
packages.
"""


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

LOG = logging.getLogger('environment_module_builder')

def pre_check(config):
    LOG.debug('Performing pre-check before installing module')
    if "PYTHONPATH" in os.environ:
        raise Exception("The PYTHONPATH environment variable must NOT be set when creating modules.")

    module_path = Path(config['variables']['module_path'])
    if module_path.exists():
        raise Exception(f"The destination path {module_path} already exists, "
                        f"please remove it and try again.")


def prep(config_path):
    LOG.debug('Preparing environment variables')
    # Write files as group and world readable
    os.umask(0o22)
    os.chdir(config_path.parent)
    os.environ['LC_ALL'] = 'en_AU.utf8'
    os.environ['LANG'] = 'C.UTF-8'


def date(date_format="%Y%m%d") -> str:
    return datetime.datetime.now().strftime(date_format)


def run(cmd: str):
    LOG.debug('Running command: %s', cmd)
    return subprocess.run(cmd, shell=True, check=True, stdout=sys.stdout, stderr=sys.stderr)


def install_conda_packages(env_file, variables):
    LOG.debug('Installing conda packages from %s', env_file)
    # make sure no ~/.local stuff interferes with the install
    os.environ['PYTHONNOUSERSITE'] = "1"

    env_name = variables['module_name']
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
    prefix = pip_conf.get('prefix',pip_conf.get('dest'))  # 'dest' for backwards compatibility
    target = pip_conf.get('target')
    requirements = pip_conf['requirements']
    if prefix and target is None:
        dest = prefix
        arg = f'--prefix {prefix}'
    elif target and prefix is None:
        dest = target
        arg = f'--target {target}'
    else:  # Either no target or prefix OR target and prefix were in the conf
        raise Exception('Either prefix: <prefix path> or target: <target path> is required by install_pip_packages:')

    LOG.debug(f'Installing pip packages from "{requirements}" into directory "{dest}"')
    run(f'{pip} install -v --no-deps {arg} --compile --requirement {requirements}')


def find_default_version(module_name):
    cmd = f"module --terse avail {module_name}"
    output = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='ascii')
    versions = [version for version in output.stdout.splitlines() if f'{module_name}/' in version]
    default_version = [version for version in versions if '(default)' in version]
    if default_version:
        return default_version[0].replace('(default)', '')
    elif len(versions) > 0:
        return versions[-1]
    else:
        raise Exception('No version of module %s is available.' % module_name)

def run_final_commands_on_module(commands, module_name):
    for command in commands:
        command = f'module load {module_name}; {command}'
        run(command)

def include_stable_module_dep_versions(config):
    stable_module_deps = config.get('stable_module_deps', [])
    for dep in stable_module_deps:
        default_version = find_default_version(dep)
        dep = dep.replace('-', '_')
        config['variables'][f'fixed_{dep}'] = default_version

def main(config_path):
    logging.basicConfig(level=logging.DEBUG)
    LOG.debug('Reading config file')
    config = read_config(config_path)
    config['variables']['module_version'] = date()
    include_templated_vars(config)
    include_stable_module_dep_versions(config)

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

    if 'finalise_commands' in config:
        module_name_and_version = variables['module_name'] + '/' + variables['module_version']
        run_final_commands_on_module(config['finalise_commands'], module_name_and_version)



    fix_module_permissions(variables['module_path'])

if __name__ == '__main__':
    main(Path(sys.argv[1]))
