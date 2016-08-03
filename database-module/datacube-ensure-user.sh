#!/usr/bin/env bash

if [ -n "$PBS_JOBID" ]; then
  exit 0
fi

db_server=$1
db_port=$2
db_user=$USER

if psql -h $db_server -p $db_port -U $db_user -w -c "select 1;" postgres > /dev/null ; then
  exit 0
fi

generated_key="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)"
real_name="$(getent passwd $db_user | cut -d: -f5)"

>&2 echo "Unable to connect to the database (${db_server}:${db_port})."
>&2 echo "Attempting to create a new user for ${db_user}."

#Create user
export PGPASSWORD=guest
if ! psql -h $db_server -p $db_port -U guest -c "select create_readonly_agdc_user('$db_user', '$generated_key', '$real_name');" > /dev/null ; then
  >&2 echo "Failed! Make sure your ~/.pgpass contains valid credentials."
  exit 1
fi

# Write pgpass
(
    umask 077;
    echo "$db_server:$db_port:*:$db_user:$generated_key" >> ~/.pgpass
)

>&2 echo "Success! Credentials written to ~/.pgpass."

