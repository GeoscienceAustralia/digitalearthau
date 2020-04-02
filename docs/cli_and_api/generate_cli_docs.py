"""

Generate RST output files for Click based CLI tools

The generated outputs require the sphinx-click extension.

"""

import pkg_resources

eps = [ep for ep in pkg_resources.iter_entry_points('console_scripts') if ep.dist.project_name == 'digitalearthau']

clis = [(ep.name, f'{ep.module_name}:{ep.attrs[0]}')
        for ep in eps]

for cmd, arg in clis:
    with open(f'{cmd}.rst', 'wt') as f:
        f.write(f"""
{cmd.title()}
{"=" * len(cmd)}

.. click:: {arg}
   :prog: {cmd}
""")
