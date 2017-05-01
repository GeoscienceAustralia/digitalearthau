#!/usr/bin/env python
#
# Ensure that a datacube user account exists for the current user
#
# It can copy credentials stored in .pgpass to connect to a different database
# Or create a new user account if required.
#
from __future__ import print_function

import os
import pwd
import random
import string
import sys
from collections import namedtuple
from pathlib import Path

import click
import psycopg2
import pytest
from boltons.fileutils import atomic_save

OLD_DB_HOST = '130.56.244.227'
PASSWORD_LENGTH = 32

DBCreds = namedtuple('DBCreds', ['host', 'port', 'database', 'username', 'password'])

CANT_CONNECT_MSG = """
Unable to connect to the database ({0.username}@{0.host}:{0.port}).
"""

USER_ALREADY_EXISTS_MSG = """
An account for '{}' already exists in the Data Cube Database, but 
we were unable to connect to it. This can happen if you have used the Data 
Cube from raijin, and are now trying to access from VDI, or vice-versa.

To fix this problem, please copy your ~/.pgpass file from the system you 
initially used to access the Data Cube, onto the current system."""


class CredentialsNotFound(Exception):
    pass


def print_stderr(msg):
    print(msg, file=sys.stderr)


def can_connect(dbcreds):
    """ Can we connect to the database defined by these credentials? """
    try:
        conn = psycopg2.connect(host=dbcreds.host,
                                port=dbcreds.port,
                                user=dbcreds.username,
                                database=dbcreds.database)
        cur = conn.cursor()
        cur.execute('SELECT 1;')
        return True
    except psycopg2.Error:
        return False


def find_credentials(pgpass, host, username):
    if not pgpass.exists():
        raise CredentialsNotFound("No existing pgpass file")

    with pgpass.open() as src:
        for line in src:
            try:
                creds = DBCreds(*line.strip().split(':'))
                if creds.host == host and creds.username == username:
                    return creds
            except TypeError:
                continue

    raise CredentialsNotFound("No legacy DB settings found")


def append_credentials(pgpass, dbcreds):
    """ Append credentials to pgpass file """
    try:
        with pgpass.open() as fin:
            data = fin.read()
    except FileNotFoundError:
        data = ''

    with atomic_save(str(pgpass.absolute()), file_perms=0o600, text_mode=True) as fout:
        if data:
            fout.write(data)
            if not data.endswith('\n'):
                fout.write('\n')
        fout.write(':'.join(dbcreds) + '\n')


_PWD = pwd.getpwuid(os.geteuid())
CURRENT_USER = _PWD.pw_name
CURRENT_REAL_NAME = _PWD.pw_gecos
CURRENT_HOME_DIR = _PWD.pw_dir


@click.command()
@click.argument('hostname')
@click.argument('port', type=click.INT)
@click.argument('username', default=CURRENT_USER, required=False)
def main(hostname, port, username):
    """
    Ensure that a user account exists in the specified Data Cube Database
    """
    if 'PBS_JOBID' in os.environ:
        return

    dbcreds = DBCreds(host=hostname, port=str(port), username=username,
                      database='datacube', password=None)
    pgpass = Path(CURRENT_HOME_DIR) / '.pgpass'

    if can_connect(dbcreds):
        # User can connect to requested database without a password, no more work to do
        return
    else:
        try:
            # We deliberately assume different ports on the same host are the same server.
            # (in our case, pgbouncer and the db itself have different ports)
            # So we don't match port when we find credentials
            creds = find_credentials(pgpass, OLD_DB_HOST, username)
            new_creds = creds._replace(host=dbcreds.host, port=dbcreds.port)
            print_stderr('Migrating you to the new database server.')
        except CredentialsNotFound:
            try:
                print_stderr('Attempting to create a new user for {}...'.format(dbcreds.username))
                new_creds = create_db_account(dbcreds)
                print_stderr('Created new database account.')
            except psycopg2.Error:
                print_stderr('Please contact a datacube administrator to help resolve user account creation.')
                return

        append_credentials(pgpass, new_creds)

        if can_connect(dbcreds):
            print_stderr("User credentials written to ~/.pgpass. Connection test: passed.")
        else:
            print_stderr('Unable to connect to the Data Cube database, please contact an administrator for help.')


def create_db_account(dbcreds):
    """ Create AGDC user account on the requested """
    real_name = CURRENT_REAL_NAME if dbcreds.username == CURRENT_USER else None

    dbcreds = dbcreds._replace(database='*', password=gen_password())
    try:
        with psycopg2.connect(host=dbcreds.host, port=dbcreds.port,
                              user='guest', database='guest', password='guest') as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT create_readonly_agdc_user(%s, %s, %s);', (dbcreds.username,
                                                                              dbcreds.password,
                                                                              real_name))
    except psycopg2.Error as err:
        if err.pgerror and 'already exists' in err.pgerror:
            print_stderr(USER_ALREADY_EXISTS_MSG.format(dbcreds.username))
        else:
            print_stderr('Error creating user account for {}: {}'.format(dbcreds.username, err))
        raise err
    return dbcreds


def gen_password(length=20):
    char_set = string.ascii_letters + string.digits
    if not hasattr(gen_password, "rng"):
        gen_password.rng = random.SystemRandom()  # Create a static variable
    return ''.join([gen_password.rng.choice(char_set) for _ in range(length)])


if __name__ == '__main__':
    if sys.version_info[0] == 2:
        sys.stderr.write("""
Warning: we may discontinue Python 2 support in the near future.

Please consider moving to our Python 3 module: agdc-py3-prod
                                                                                                                                                       
  -> If you have a hard requirement on Python 2 that makes the change 
     difficult, please notify us at simon.oliver@ga.gov.au
  -> The python-modernize command is available to ease conversions, 
     see: https://python-modernize.readthedocs.io
""")
        sys.stderr.flush()
    main()


#########
# Tests #
#########


def test_no_pgpass(tmpdir):
    path = tmpdir.join('pgpass.txt')
    path = Path(str(path))

    assert not path.exists()

    # No pgpass file exists
    with pytest.raises(CredentialsNotFound):
        find_credentials(path, host='130.56.244.227', username='foo_user')

    creds = DBCreds('127', '1234', 'datacube', 'username', 'password')

    append_credentials(path, creds)

    assert path.exists()
    with path.open() as src:
        contents = src.read()
    assert contents == ':'.join(creds) + '\n'


def test_append_credentials(tmpdir):
    existing_line = '130.56.244.227:5432:*:foo_user:asdf'
    pgpass = tmpdir.join('pgpass.txt')
    pgpass.write(existing_line)

    path = Path(str(pgpass))
    creds = find_credentials(pgpass, host='130.56.244.227', username='foo_user')

    assert creds is not None
    assert creds.password == 'asdf'

    new_creds = creds._replace(host='127')

    append_credentials(path, new_creds)

    with path.open() as src:
        contents = src.read()

    expected = existing_line + '\n' + existing_line.replace('130.56.244.227', '127') + '\n'
    assert contents == expected
