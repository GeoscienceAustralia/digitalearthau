#!/usr/bin/env python
#
# Ensure that a datacube user account exists for the current user
#
# It can copy credentials stored in .pgpass to connect to a different database
# Or create a new user account if required.
#
from collections import namedtuple
import os
import click
import random
import pwd
from boltons.fileutils import atomic_save
from pathlib import Path
import string
import sys
import psycopg2

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
        conn = psycopg2.connect(host=dbcreds.host, port=dbcreds.port, user=dbcreds.username)
        cur = conn.cursor()
        cur.execute('SELECT 1;')
        return True
    except psycopg2.Error:
        return False


def find_credentials(pgpass, host, username):
    with pgpass.open() as src:
        for line in src:
            try:
                creds = DBCreds(*line.strip().split(':'))
                if creds.host == host and creds.username == username:
                    return creds
            except TypeError:
                continue
    raise CredentialsNotFound()


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


@click.command()
@click.argument('hostname')
@click.argument('port', type=click.INT)
@click.argument('username', default=os.environ['USER'], required=False)
def main(hostname, port, username):
    """
    Ensure that a user account exists in the specified Data Cube Database
    """
    if 'PBS_JOBID' in os.environ:
        return

    dbcreds = DBCreds(host=hostname, port=port, username=username, database=None, password=None)
    pgpass = Path(os.environ['HOME']) / '.pgpass'

    if can_connect(dbcreds):
        # User can connect to requested database without a password, no more work to do
        return
    else:
        print_stderr(CANT_CONNECT_MSG.format(dbcreds))
        try:
            creds = find_credentials(pgpass, OLD_DB_HOST, username)
            new_creds = creds._replace(host=dbcreds.host)
            print_stderr('Existing credentials found. Copying from old database server in ~/.pgass.')
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
            print_stderr("Success! Credentials written to ~/.pgpass and connection tested and works.")
        else:
            print_stderr('Still unable to connect to the Data Cube database, please contact an administrator for help.')


def create_db_account(dbcreds):
    """ Create AGDC user account on the requested """
    real_name = get_real_name()
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


def get_real_name():
    uid = os.getuid()
    info = pwd.getpwuid(uid)
    return info.pw_gecos


def gen_password(length=20):
    char_set = string.ascii_letters + string.digits
    if not hasattr(gen_password, "rng"):
        gen_password.rng = random.SystemRandom()  # Create a static variable
    return ''.join([gen_password.rng.choice(char_set) for _ in range(length)])


if __name__ == '__main__':
    main()

#########
# Tests #
#########


def test_no_pgpass(tmpdir):
    path = tmpdir.join('pgpass.txt')
    path = Path(str(path))

    assert not path.exists()
    #
    # creds = find_credentials(path)
    #
    # assert creds == None

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
