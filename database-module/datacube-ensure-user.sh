#!/usr/bin/env bash

db_server=130.56.244.227
db_user=$USER

if psql -h $db_server -U $db_user -w -c "select 1;" postgres > /dev/null ; then
  exit 0
fi

generated_key="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)"
real_name="$(getent passwd $db_user | cut -d: -f5)"

#Create user
if ! psql -h $db_server -U guest -w -c "select create_readonly_agdc_user('$db_user', '$generated_key', '$real_name');" > /dev/null ; then
  exit 1
fi

# Write pgpass
(
    umask 077;
    echo "$db_server:5432:*:$db_user:$generated_key" >> ~/.pgpass
    echo "$db_server:6432:*:$db_user:$generated_key" >> ~/.pgpass
)

