#!/usr/bin/env python


import time
from pathlib import Path
from subprocess import check_output

SUBMIT_THROTTLE_SECS = 1


def main(name: str, folder: Path, submit_limit: int, concurrent_jobs=4):
    folder = folder.absolute()

    run_directory = Path('runs').absolute()

    if not folder.exists():
        raise ValueError("Folder doesn't exist: %s" % folder)

    # For input folder, get list of tiles, do one qsub per X (?)
    xs = set(int(p.name.split('_')[0]) for p in folder.iterdir() if p.name != 'ncml')

    print("Found %s total jobs" % len(xs))
    xs = sorted(xs)

    # Keys: from 0 to concurrent_jobs-1
    # Values: the last jobid to be submitted in that position.
    last_job_slots = {}

    for i, x in enumerate(xs):
        if i == submit_limit:
            print("Submit limit ({}) reached, done.".format(submit_limit))
            break

        subjob_name = '{}_{}'.format(name, x)

        output_path = run_directory.joinpath('{}.tsv'.format(subjob_name))
        error_path = run_directory.joinpath('{}.log'.format(subjob_name))

        if output_path.exists():
            print("[{}] {}: output exists, skipping".format(i, subjob_name))
            continue

        last_job_id = last_job_slots.get(i % concurrent_jobs)

        requirements = []
        if last_job_id:
            requirements.extend(['-W', 'depend=afterany:{}'.format(str(last_job_id).strip())])

        sync_command = [
            'python', '-m', 'datacubenci.sync', '-j', '4',  # '--trash-missing', '--trash-archived',
            # glob all the paths starting with X_
            *(map(str, list(folder.glob('{}_*'.format(x)))))
        ]

        output = check_output([
            'qsub', '-V',
            '-P', 'v10',
            '-q', 'express',
            '-l', 'walltime=20:00:00,mem=4GB,ncpus=2,jobfs=1GB,other=gdata',
            '-l', 'wd',
            '-N', 'sync-{}'.format(subjob_name),
            '-m', 'e',
            '-M', 'jeremy.hooke@ga.gov.au',
            '-e', str(error_path),
            '-o', str(output_path),
            *requirements,
            '--',
            *sync_command
        ])

        job_id = output.decode('utf-8').strip(' \\n')
        print("[{}] {}: submitted {}".format(i, subjob_name, job_id))
        last_job_slots[i % concurrent_jobs] = job_id

        time.sleep(SUBMIT_THROTTLE_SECS)


if __name__ == '__main__':
    main('5fc', Path('/g/data/fk4/datacube/002/LS5_TM_FC'), 4)
