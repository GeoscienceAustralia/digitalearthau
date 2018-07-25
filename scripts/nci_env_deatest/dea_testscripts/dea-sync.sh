#!/bin/bash
set -eu

MODULE=$1
PROJECT=v10
QUEUE=express
YEAR=$2
PRODUCT=$3
TRASH_ARC=$4
PATH_TO_PROCESS=$5
TBASE=$6
CONFIGFILE="$TBASE"/../datacube_config.conf

WORKDIR="$TBASE"/work/sync/"${PRODUCT}"
SYNC_CACHE="${WORKDIR}"/cache/
SUBMISSION_LOG="$WORKDIR"/sync-submission-$(date '+%F-%T').log
JOB_NAME="Sync_${PRODUCT}_${YEAR}"
count=0
dbhostname='agdcdev-db.nci.org.au'
dbport='6432'
dbname='datacube'

mkdir -p "${SYNC_CACHE}"
echo "$PRODUCT Sync start time: " "$(date '+%F-%T')" > "$SUBMISSION_LOG"

module use /g/data/v10/public/modules/modulefiles
module load "${MODULE}"
export DATACUBE_CONFIG_PATH="$CONFIGFILE"

while read -r LINE; do
  if [[ "$count" -eq 1 ]]
  then
      dbhostname="$(cut -d' ' -f2 <<<"$LINE")"
      count=$((count+1))
  elif [[ "$count" -eq 2 ]]
  then
      dbport="$(cut -d' ' -f2 <<<"$LINE")"
      count=$((count+1))
  elif [[ "$count" -eq 3 ]]
  then
      dbname="$(cut -d' ' -f2 <<<"$LINE")"
      count=$((count+1))
  fi

  if [[ "$LINE" == "[datacube]" ]]; then
      count=$((count+1))
  fi
done  < "$DATACUBE_CONFIG_PATH"

TRASH_ARCHIVED=''
if [ "$TRASH_ARC" == yes ]; then
   TRASH_ARCHIVED='--trash-archived'
fi

cd "${WORKDIR}" || exit 0

nohup "$SHELL" >> "$SUBMISSION_LOG" 2>&1 << EOF &
echo "Logging job: ${JOB_NAME} into: ${SUBMISSION_LOG}"
echo ""
echo Loading module "${MODULE}"
echo ""

echo "********************************************************************"
echo "  Datacube Config Path (sync):  $DATACUBE_CONFIG_PATH" 
echo "********************************************************************"
echo ""

# Check if we can connect to the database
datacube -vv system check

# Read agdc datasets from the database before sync process
echo ""
echo "**********************************************************************"
echo "Read previous agdc_dataset product names and count before sync process"
echo "Connected to the database host name: $dbhostname"
echo "Connected to the database port number: $dbport"
echo "Connected to the database name: $dbname"
psql -h "${dbhostname}" -p "${dbport}"  -d "${dbname}"  -c 'select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name'
echo "**********************************************************************"
echo ""
echo "Starting Sync process......"

##################################################################################################
# Run dea-sync process
##################################################################################################
qsub -V -N "${JOB_NAME}" -q "${QUEUE}" -W block=true -l wd,walltime=10:00:00,mem=25GB,ncpus=1 -P "${PROJECT}" -o "$WORKDIR" -e "$WORKDIR" -- dea-sync -vvv --cache-folder "$WORKDIR"/cache -j 4 --log-queries "$TRASH_ARCHIVED" --update-locations --index-missing "$PATH_TO_PROCESS"
EOF
