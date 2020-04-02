"""

Generate RST output files for Click based CLI tools

The generated outputs require the sphinx-click extension.

"""

import pkg_resources

eps = [ep for ep in pkg_resources.iter_entry_points('console_scripts') if ep.dist.project_name == 'digitalearthau']

clis = [(ep.name.split('-', 1)[1], ep.name, f'{ep.module_name}:{ep.attrs[0]}')
        for ep in eps]

for name, cmd, arg in clis:
    with open(f'{name}.rst', 'wt') as f:
        f.write(f"""
{name.title()}
{"=" * len(name)}

.. click:: {arg}
   :prog: {cmd}
""")
