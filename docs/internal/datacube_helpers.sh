mk_db_sql () {
    local db_name=${1}
    cat <<EOF
CREATE DATABASE ${db_name}
WITH
OWNER = agdc_admin
ENCODING = 'UTF8'
LC_COLLATE = 'en_AU.UTF-8'
LC_CTYPE = 'en_AU.UTF-8'
TABLESPACE = pg_default
CONNECTION LIMIT = -1;

GRANT ALL ON DATABASE ${db_name} TO agdc_admin;
GRANT CONNECT, TEMPORARY ON DATABASE ${db_name} TO PUBLIC;
GRANT ALL ON DATABASE ${db_name} TO test;
ALTER DATABASE ${db_name} SET search_path TO "\$user", public, agdc;

EOF
}

mk_dev_config () {
    local db_name=$1
    local f_name=${2-"${db_name}.conf"}
    cat > ${f_name} <<EOF
[datacube]
db_hostname: agdcdev-db.nci.org.au
db_port: 6432
db_database: ${db_name}
EOF
}

