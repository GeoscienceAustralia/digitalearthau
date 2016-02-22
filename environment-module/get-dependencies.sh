#!/usr/bin/env bash

rm -rf agdc-v2 > /dev/null 2>&1
git clone https://github.com/data-cube/agdc-v2/ > /dev/null 2>&1
python << EOF
import re
p = '(.*)install_requires(\\s*)=(\\s*)\\[(.*?)\\].*'
setup = open("agdc-v2/setup.py").read()
m = re.match(p, setup, re.S)
x = x = re.sub(r'\\s*', '', m.group(4)).replace('"', '').replace("'", '').replace(',', ' ').strip()
x = re.sub(r'dask.*? ', '', x)
x = re.sub(r'matplotlib.*? ', '', x)
print x
EOF
rm -rf agdc-v2 > /dev/null 2>&1
