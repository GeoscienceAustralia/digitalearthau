#!/usr/bin/env bash

if [ -n "$PBS_JOBID" ]; then
  exit 0
fi

db_server=$1
db_port=$2
db_user=$USER

if psql -h $db_server -p $db_port -U $db_user -w -c "select 1;" postgres > /dev/null 2>&1 ; then
  exit 0
fi

>&2 echo "Unable to connect to the database (${db_user}@${db_server}:${db_port})."
>&2 echo
>&2 echo "Attempting to create a new user for ${db_user}..."
>&2 echo

generated_key=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
real_name=$(getent passwd $db_user | cut -d: -f5)

user_already_exists_msg="An account for '$db_user' already exists, but we were unable to connect to it.\n
 -> This can happen if you have used the Datacube from raijin, and are now trying to access from VDI, or vice-versa.\n
 -> To fix this problem, please copy your ~/.pgpass file from the system you initially used to access the datacube, onto the current system.\n
"

#Create user
export PGPASSWORD=guest
create_user_output=$((psql -h $db_server -p $db_port -U guest guest -v db_user="$db_user" -v generated_key="$generated_key" -v real_name="$real_name" <<< "select create_readonly_agdc_user(:'db_user', :'generated_key', :'real_name');") 2>&1)
if [[ $? != 0 ]]; then
    if [[ $create_user_output == *"already exists"* ]]; then
        printf "$user_already_exists_msg" | fmt - 1>&2
    else
        >&2 echo "Error creating user account for '$db_user'. "
        >&2 echo ">> " $create_user_output
    fi
    exit 1
fi


# Write pgpass
(
    umask 077;
    echo "$db_server:$db_port:*:$db_user:$generated_key" >> ~/.pgpass
)

>&2 echo "Success! Credentials written to ~/.pgpass."

