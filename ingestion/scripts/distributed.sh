#!/usr/bin/env bash

env_script=/etc/bashrc
ppn=1
tpp=1
umask=0027

while [[ $# > 0 ]]
do
    key="$1"
    case $key in
    --help)
        echo Usage: $0 --env init_env.sh --umask ${umask} --ppn ${ppn} --tpp ${tpp} script args
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

init_env="umask ${umask};\
source /etc/bashrc;\
source ${env_script}"

echo "*** ENVIRONMENT ***"
cat ${env_script}

eval ${init_env}

SCHEDULER_NODE=`sed '1q;d' $PBS_NODEFILE`
SCHEDULER_PORT=`shuf -i 2000-65000 -n 1`
SCHEDULER_ADDR=$SCHEDULER_NODE:$SCHEDULER_PORT

pbsdsh -n 0 -- /bin/bash -c "${init_env};\
dscheduler --port $SCHEDULER_PORT"&
sleep 5s

for ((i=0; i<PBS_NCPUS; i+=NCPUS)); do
  pbsdsh -n $i -- /bin/bash -c "${init_env};\
dworker $SCHEDULER_ADDR --nprocs ${ppn} --nthreads ${tpp}"&
done
sleep 5s

echo "*** APPLICATION ***"
echo "${@/DSCHEDULER/${SCHEDULER_ADDR}}"

"${@/DSCHEDULER/${SCHEDULER_ADDR}}"

