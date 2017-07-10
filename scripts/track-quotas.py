#!/g/data/v10/public/modules/agdc-py3-env/20170627/envs/agdc/bin/python

import subprocess
import csv
import re
from pathlib import Path
from datetime import datetime

projects = ['rs0', 'v10', 'u46', 'fk4', 'r78']
filesystems = ['gdata1', 'gdata2', 'gdata3', 'short', 'massdata']

cpu_fields = ['cpu_grant', 'cpu_usage', 'cpu_avail', 'cpu_bonus_used']
storage_fields = ('grant', 'usage', 'avail', 'igrant', 'iusage', 'iavail')

FIELDNAMES = ['time', 'project', 'period'] + cpu_fields + ['{}_{}'.format(fs, field) for fs in filesystems for field in storage_fields]


def update_project(data):
    fname = data['project'] + '.csv'

    if not Path(fname).exists():
        with open(fname, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
            writer.writeheader()

    with open(fname, 'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writerow(data)

def project_usage(output):
    usage = {
        'project': re.findall(r'.*Project=([\w\d]+)', output)[0],
        'period': re.findall(r'.*Compute Period=(.*?) .*', output, re.MULTILINE)[0],
    }
    try:
        cpu = {
            'cpu_grant': re.findall(r'.*Total Grant:\s+([\d\.]+) KSU.*', output, re.MULTILINE)[0],
            'cpu_usage': re.findall(r'.*Total Used:\s+([\d\.]+) KSU.*', output, re.MULTILINE)[0],
            'cpu_avail': re.findall(r'.*Total Avail:\s+([\d\.]+) KSU.*', output, re.MULTILINE)[0],
            'cpu_bonus_used': re.findall(r'.*Bonus Used:\s+([\d\.]+) KSU.*', output, re.MULTILINE)[0]
        }
        usage.update(cpu)
    except:
        pass
    for filesystem in filesystems:
        storage = storage_usage(filesystem, output)
        usage.update(storage)
    return usage

def storage_usage(storage_pt, text):
    vals = re.findall('''%s\s+([\d\w\.]+)
                           \s+([\d\w\.]+)
                           \s+([\d\w\.]+)
                           \s+([\d\.]+[KM])
                           \s+([\d\.]+[KM])
                           \s+([\d\.]+[KM])''' % storage_pt, text, re.MULTILINE | re.X)
    if vals:
        vals = vals[0]
        out = {
            '%s_grant' % storage_pt: human2bytes(vals[0]),
            '%s_usage' % storage_pt: human2bytes(vals[1]),
            '%s_avail' % storage_pt: human2bytes(vals[2]),
            '%s_igrant' % storage_pt: human2decimal(vals[3]),
            '%s_iusage' % storage_pt: human2decimal(vals[4]),
            '%s_iavail' % storage_pt: human2decimal(vals[5]),
        }
        return out
    return {}

def main():
    for project in projects:
        cp = subprocess.run(['nci_account', '-P', project], stdout=subprocess.PIPE)
        usage = project_usage(cp.stdout.decode('ascii'))
        assert project == usage['project']
        usage['time'] = str(datetime.now())
        update_project(usage)


def human2decimal(s):
    unit = s[-1]
    val = float(s[:-1])
    if unit == 'K':
        return int(val * 1000)
    elif unit == 'M':
        return int(val * 1000000)


"""
Bytes-to-human / human-to-bytes converter.
Based on: http://goo.gl/kTQMs
Working with Python 2.x and 3.x.

Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
License: MIT
"""

# see: http://goo.gl/kTQMs
SYMBOLS = {
    'customary'     : ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'),
    'customary_ext' : ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                       'zetta', 'iotta'),
    'iec'           : ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext'       : ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                       'zebi', 'yobi'),
}

def human2bytes(s):
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    When unable to recognize the format ValueError is raised.

      >>> human2bytes('0 B')
      0
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1 M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776

      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'
    """
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]:1}
    for i, s in enumerate(sset[1:]):
        prefix[s] = 1 << (i+1)*10
    return int(num * prefix[letter])

if __name__ == '__main__':
    main()
