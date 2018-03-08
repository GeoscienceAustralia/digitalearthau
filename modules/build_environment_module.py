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

import yaml

MODULE_DIR = '/g/data/v10/public/modules'
MINICONDA_URL = 'https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh'


def pre_check(config):
    if "PYTHONPATH" in os.environ:
        raise Exception("The PYTHONPATH environment variable must NOT be set when creating modules.")

    module_path = Path(config['variables']['module_path'])
    if module_path.exists():
        raise Exception(f"The destination path {module_path} already exists, "
                        f"please remove it and try again.")


#    "LOADEDMODULES=pbs"
#    "MODULEPATH=/g/data/v10/public/modules/modulefiles:/apps/.mf:/opt/Modules/modulefiles:/apps/Modules/modulefiles:"

def prep(config_path):
    # Write files as group and world readable
    os.umask(0o22)
    os.chdir(config_path.parent)
    os.environ['LC_ALL'] = 'en_AU.utf8'
    os.environ['LANG'] = 'C.UTF-8'


def date(date_format="%Y%m%d") -> str:
    return datetime.datetime.utcnow().strftime(date_format)


def make_output_dir(base_path) -> Path:
    today = date()

    output_dir = base_path / today

    while output_dir.exists():
        index = 1
        output_dir = base_path / f"{today}_{index}"

    output_dir.mkdir(exist_ok=False)
    return output_dir


def run(cmd: str):
    return subprocess.run(cmd, shell=True, check=True, stdout=sys.stdout, stderr=sys.stderr)


def install_conda(conda_conf):
    destination_path = Path(conda_conf['dest'])
    with tempfile.TemporaryDirectory() as tmpdir:
        miniconda_installer = tmpdir / "miniconda.sh"
        urllib.request.urlretrieve(conda_conf['url'], miniconda_installer)

        run(f"{miniconda_installer} -b -p {destination_path}")

    conda = destination_path / "bin/conda"

    run(f"{conda} config --prepend channels conda-forge --system")
    # update root env to the latest python and packages
    run(f"${conda} update --all - y")


def install_conda_packages(conda_bin_path, from_file):
    # make sure no .local stuff interferes with the install
    os.environ['PYTHONNOUSERSITE'] = "1"

    run(f"{conda_bin_path} install -v --yes --file {from_file}")


def write_template(template_file, variables, output_file):
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

        shutil.copy(src, dest)

        if 'chmod' in task:
            dest.chmod(task['chmod'])


def read_config(path):
    return yaml.safe_load(path)


def copy_and_fill_templates(template_tasks, variables):
    for task in template_tasks:
        fill_templates_from_variables(task, variables)

        src = Path(task['src'])
        dest = Path(task['dest'])
        write_template(src, variables, dest)

        if 'chmod' in task:
            dest.chmod(task['chmod'])


def include_templated_vars(config):
    fill_templates_from_variables(config['templated_variables'], config['variables'])
    config['variables'].update(config['templated_variables'])

    del config['templated_variables']


def fix_module_permissions(module_path):
    run(f'chmod -R a-w "${module_path}"')


def install_pip_packages(pip_conf, variables):
    fill_templates_from_variables(pip_conf, variables)
    pip = pip_conf['pip_cmd']
    dest = pip_conf['dest']
    requirements = pip_conf['requirements']
    run(f'{pip} install --no-deps --target {dest} --requirement {requirements}')


def main(config_path):
    config = read_config(config_path)
    config['variables']['module_version'] = date()
    include_templated_vars(config)

    variables = config['variables']

    pre_check(config)
    prep(config_path)

    if 'install_conda' in config:
        install_conda(config['install_conda'])

    if 'install_conda_packages' in config:
        install_conda_packages(variables['conda_bin_path'], config['install_conda_packages'])

    if 'install_pip_packages' in config:
        install_pip_packages(config['install_pip_packages'], variables)

    copy_files(config.get('copy_files', []), variables)
    copy_and_fill_templates(config.get('template_files', []), variables)
    fix_module_permissions(variables['module_path'])

    if __name__ == '__main__':
        main(Path(sys.argv[1]))
