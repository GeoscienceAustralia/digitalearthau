#!/usr/bin/env bash

set -eu

# The specific module+version that each node should load.
module_name=$1
shift

ppn=1
tpp=1
mem=4e9
umask=0027

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
    --help)
        echo "Usage: $0 <dea_module> --umask ${umask} --ppn ${ppn} --tpp ${tpp} script args"
        exit 0
        ;;
    --umask)
        umask="$2"
        shift
        ;;
    --ppn)
        ppn="$2"
        shift
        ;;
    --tpp)
        tpp="$2"
        shift
        ;;
    *)
    break
    ;;
    esac
shift
done

init_env="umask ${umask}; source /etc/bashrc; module use /g/data/v10/public/modules/modulefiles/; module use /g/data/v10/private/modules/modulefiles/; module load ${module_name}"

echo "Using DEA module: ${module_name}"

# Make lenient temporarily: global bashrc/etc can reference unassigned variables.
set +u
eval "${init_env}"
set -u

SCHEDULER_NODE=$(sed '1q;d' "$PBS_NODEFILE")
SCHEDULER_PORT=$(shuf -i 2000-65000 -n 1)
SCHEDULER_ADDR=$SCHEDULER_NODE:$SCHEDULER_PORT

n0ppn=$(( ppn < NCPUS-2 ? ppn : NCPUS-2 ))
n0ppn=$(( n0ppn > 0 ? n0ppn : 1 ))

pbsdsh -n 0 -- /bin/bash -c "${init_env}; dask-scheduler --port $SCHEDULER_PORT"&
sleep 5s

pbsdsh -n 0 -- /bin/bash -c "${init_env}; dask-worker $SCHEDULER_ADDR --nprocs ${n0ppn} --nthreads ${tpp} --memory-limit ${mem}"&
sleep 0.5s

for ((i=NCPUS; i<PBS_NCPUS; i+=NCPUS)); do
  pbsdsh -n $i -- /bin/bash -c "${init_env}; dask-worker $SCHEDULER_ADDR --nprocs ${ppn} --nthreads ${tpp} --memory-limit ${mem}"&
  sleep 0.5s
done
sleep 5s

echo "*** APPLICATION ***"
echo "${@/DSCHEDULER/${SCHEDULER_ADDR}}"
echo
echo "*** Datacube Check ***"
datacube -vv system check
set | grep -i datacube
echo "PATH=$PATH"

"${@/DSCHEDULER/${SCHEDULER_ADDR}}"

