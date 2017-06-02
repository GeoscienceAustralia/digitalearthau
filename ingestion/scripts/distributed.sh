#!/usr/bin/env bash

env_script=${module_dest}/scripts/environment.sh
ppn=1
tpp=1
umask=0027

while [[ $# > 0 ]]
do
    key="$1"
    case $key in
    --help)
        echo Usage: $0 --env ${env_script} --umask ${umask} --ppn ${ppn} --tpp ${tpp} script args
        exit 0
        ;;
    --env)
        env_script="$2"
        shift
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

init_env="umask ${umask}; source /etc/bashrc; source ${env_script}"

echo "*** ENVIRONMENT ***"
cat ${env_script}

eval ${init_env}

SCHEDULER_NODE=`sed '1q;d' $PBS_NODEFILE`
SCHEDULER_PORT=`shuf -i 2000-65000 -n 1`
SCHEDULER_ADDR=$SCHEDULER_NODE:$SCHEDULER_PORT

n0ppn=$(( $ppn < $NCPUS-2 ? $ppn : $NCPUS-2 ))
n0ppn=$(( $n0ppn > 0 ? $n0ppn : 1 ))

pbsdsh -n 0 -- /bin/bash -c "${init_env}; dask-scheduler --port $SCHEDULER_PORT"&
sleep 5s

pbsdsh -n 0 -- /bin/bash -c "${init_env}; dask-worker $SCHEDULER_ADDR --nprocs ${n0ppn} --nthreads ${tpp}"&
sleep 0.5s

for ((i=NCPUS; i<PBS_NCPUS; i+=NCPUS)); do
  pbsdsh -n $i -- /bin/bash -c "${init_env}; dask-worker $SCHEDULER_ADDR --nprocs ${ppn} --nthreads ${tpp}"&
  sleep 0.5s
done
sleep 5s

echo "*** APPLICATION ***"
echo "${@/DSCHEDULER/${SCHEDULER_ADDR}}"

"${@/DSCHEDULER/${SCHEDULER_ADDR}}"

