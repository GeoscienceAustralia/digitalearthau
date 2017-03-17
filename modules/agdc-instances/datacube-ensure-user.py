#!/usr/bin/env python
#
# Ensure that a datacube user account exists for the current user
#
# It can copy credentials stored in .pgpass to connect to a different database
# Or create a new user account if required.
#

from collections import namedtuple
import os
import random
import pwd
from pathlib import Path
import string
import sys
import psycopg2

OLD_DB_HOST = '130.56.244.227'
PASSWORD_LENGTH = 32

DBCreds = namedtuple('DBCreds', ['host', 'port', 'database', 'username', 'password'])

CANT_CONNECT_MSG = """
Unable to connect to the database ({username}@{host}:{port}).

Attempting to create a new user for {username}..."""

USER_ALREADY_EXISTS_MSG = """
An account for '{username}' already exists in the Data Cube Database, but 
we were unable to connect to it. This can happen if you have used the Data 
Cube from raijin, and are now trying to access from VDI, or vice-versa.

To fix this problem, please copy your ~/.pgpass file from the system you 
initially used to access the Data Cube, onto the current system."""


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


def find_credentials(pgpass, old_host=OLD_DB_HOST):
    with pgpass.open() as src:
        filedata = [line.strip() for line in src]

    existing_db = [line for line in filedata
                   if old_host in line][0]

    return DBCreds(*existing_db.split(':'))


def append_credentials(pgpass, dbcreds):
    """ Append credentials to pgpass file """
    try:
        with pgpass.open() as src:
            filedata = [line.strip() for line in src]
    except FileNotFoundError:
        filedata = []

    new_creds_line = ':'.join(tuple(dbcreds))
    filedata.append(new_creds_line)

    os.umask(0o077)
    with pgpass.open('w') as dest:
        for line in filedata:
            dest.write(line + '\n')


def main():
    if 'PBS_JOBID' in os.environ:
        return

    dbcreds = DBCreds(host=sys.argv[1], port=sys.argv[2], username=os.environ['USER'], database=None, password=None)
    pgpass = Path(os.environ['HOME']) / '.pgpass'

    if can_connect(dbcreds):
        return
    else:
        print_stderr(CANT_CONNECT_MSG.format(**dbcreds._asdict()))
        try:
            creds = find_credentials(pgpass)
            new_creds = creds._replace(host=dbcreds.host)
        except:
            new_creds = create_db_account(dbcreds)

        append_credentials(pgpass, new_creds)

        print_stderr("Success! Credentials written to ~/.pgpass.")


def create_db_account(dbcreds):
    """ Create AGDC user account on the requested """
    password = gen_password()
    real_name = get_real_name()
    try:
        with psycopg2.connect(host=dbcreds.host, port=dbcreds.port, user='guest', database='guest') as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT create_readonly_agdc_user(%s, %s, %s);', (dbcreds.username,
                                                                              dbcreds.password,
                                                                              real_name))
    except psycopg2.Error as err:
        if 'already exists' in err.pgerror:
            print_stderr(USER_ALREADY_EXISTS_MSG.format(dbcreds.username))
        else:
            print_stderr('Error creating user account for {}: {}'.format(dbcreds.username, err))
    return dbcreds._replace(database='*', password=password)


def get_real_name():
    uid = os.getuid()
    info = pwd.getpwuid(uid)
    return info.pw_gecos


def gen_password(length):
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


def test_append_credentials(tmpdir):
    existing_line = '130.56.244.227:5432:*:dra547:asdf'
    pgpass = tmpdir.join('pgpass.txt')
    pgpass.write(existing_line)

    path = Path(str(pgpass))

    creds = find_credentials(pgpass)

    assert creds != None
    assert creds.password == 'asdf'
    
    new_creds = creds._replace(host='127')

    append_credentials(path, new_creds)

    with path.open() as src:
        contents = src.read()

    expected = existing_line + '\n' + existing_line.replace('130.56.244.227', '127')
    assert contents == expected

