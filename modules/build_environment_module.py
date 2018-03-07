#!/usr/bin/env python3

from pathlib import Path
import datetime
import os
import urllib.request
import tempfile
import subprocess

MODULE_DIR = '/g/data/v10/public/modules'
MINICONDA_URL = 'https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh'

env_definition = {
    'modules_dir': '/g/data/v10/public/modules',
    'module_name': 'dea-env',
    'install_miniconda': {
        'url': 'https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh'
    },
    'install_conda_deps': 'environment.yaml',
    'copy_files': [{
        'src': 'dea-env/environment.yaml',
        'dest': '/g/data/v10/public/modules/dea-env/${module_version}/environment.yaml'
    }],
    'template_files': [{
        'src': 'dea-env/modulefile.template',
        'dest': '/g/data/v10/public/modules/modulefiles/dea-env/${module_version}'
    }]
}

dea_definition = {
    'module_deps': ['udunits', 'dea-env']
    'copy_files': [{
        'src': 'dea/requirements.yaml',
        'dest': '/g/data/v10/public/modules/dea/${module_version}/requirements.yaml'
    }, {
        'src': 'dea/datacube.conf',
        'dest': '/g/data/v10/public/modules/dea/${module_version}/datacube.conf.yaml'
    }, {
        'src': 'dea/datacube-ensure-user.py',
        'dest': '/g/data/v10/public/modules/dea/${module_version}/bin/datacube-ensure-user.py'
    }],
    'template_files': [{
        'src': 'dea/modulefile.template',
        'dest': '/g/data/v10/public/modules/modulefiles/dea/${module_version}'
    }]
}


def pre_check():
    if "PYTHONPATH" in os.environ:
        raise Exception("The PYTHONPATH environment variable must not be set when creating modules.")
#    "LOADEDMODULES=pbs"
#    "MODULEPATH=/g/data/v10/public/modules/modulefiles:/apps/.mf:/opt/Modules/modulefiles:/apps/Modules/modulefiles:"


def date(format="%Y%m%d"):
    return datetime.datetime.utcnow().strftime(format)


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

def install_miniconda(package_dest):
    with tempfile.TemporaryDirectory() as tmpdir:
        miniconda_installer = tmpdir / "miniconda.sh"
        urllib.request.urlretrieve(MINICONDA_URL, miniconda_installer)
        run(f"{miniconda_installer} -b -p {package_dest}")

        conda = package_dest / "bin/conda"

        run(f"{conda} config --prepend channels conda-forge --system")
        # update root env to the latest python and packages
        run(f"${conda} update - -all - y")

        # make sure no .local stuff interferes with the install
        os.environ('PYTHONNOUSERSITE') = "1"

        run(f"{conda} install -v --yes --file {package_dest}/environment.yaml")

def write_template(template_file, vars, output_file):
    template_contents = template_file.read_text()
    template = string.Template(template_contents)
    output_file.write_text(template.substitute(vars))




def main(module_definition):
    pre_check()
    install_miniconda()


if __name__ == '__main__':
    main(env_definition)