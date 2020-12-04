

content = open('../../setup.py').readlines()

console_scripts = []
take_lines = False
for line in content:
    if ']' in line:
        take_lines = False
    if take_lines:
        console_scripts.append(line.replace("'", "").replace(",", "").strip())
    if 'console_scripts' in line:
        take_lines = True


template = """
## {name} Reference


::: mkdocs-click
    :module: {module}
    :command: {func}
"""



def line_to_tupl(line):
    name, pkg = line.split(' = ')
    module, func = pkg.split(':')
    return name, module, func
    
cli_tools = map(line_to_tupl, console_scripts)

for name, module, func in cli_tools:
    with open(f"{name}.md", 'w') as md_file:
        md_file.write(template.format(name=name, module=module, func=func))
