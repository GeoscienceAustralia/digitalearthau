#!/usr/bin/env python


import logging
import os
import time
from pathlib import Path
from subprocess import check_output
from typing import Mapping, List, Optional, Tuple, Iterable

import click
from boltons import fileutils

from datacube.index import index_connect
from datacubenci import collections
from datacubenci.index import AgdcDatasetPathIndex
from datacubenci.sync import scan

SUBMIT_THROTTLE_SECS = 1

FILES_PER_JOB_CUTOFF = 15000

_LOG = logging.getLogger(__name__)


class SyncSubmission(object):
    def __init__(self, cache_path: Path, project='v10', queue='normal', dry_run=False, verbose=True, workers=4) -> None:
        self.project = project
        self.queue = queue
        self.dry_run = dry_run
        self.verbose = verbose
        self.workers = workers
        self.cache_path = cache_path

    def warm_cache(self, tile_path: Path):

        # Update cached path list ahead of time, so PBS jobs don't waste time doing it themselves.
        click.echo("Checking path list, this may take a few minutes...")
        scan.build_pathset(get_collection(tile_path), cache_path=self.cache_path)

    def submit(self,
               input_folders: List[Path],
               output_file: Path,
               error_file: Path,
               job_name: str,
               require_job_id: Optional[int]) -> str:

        # Output files readable by others.
        attributes = ['umask=33']

        sync_opts = []
        if require_job_id:
            attributes.extend(['depend=afterok:{}'.format(str(require_job_id).strip())])
        if self.verbose:
            sync_opts.append('-v')
        if not self.dry_run:
            # For tile products like the current FC we trust the index over the filesystem.
            # (jobs that failed part-way-through left datasets on disk and were not indexed)
            sync_opts.extend(['--trash-missing', '--trash-archived', '--update-locations'])
            # Scene products are the opposite:
            # Only complete scenes are written to fs, so '--index-missing' instead of trash.
            # (also want to '--update-locations' to fix any moved datasets)

        sync_command = [
            'python', '-m', 'datacubenci.sync',
            '-j', str(self.workers),
            '--cache-folder', str(self.cache_path),
            *sync_opts,
            *(map(str, input_folders))
        ]
        qsub_opts = []
        notify_email = os.environ.get('COMPLETION_NOTIFY_EMAIL')
        if notify_email:
            qsub_opts.extend([
                '-M', notify_email
            ])

        command = [
            'qsub', '-V',
            '-P', self.project,
            '-q', self.queue,
            '-l', 'walltime=20:00:00,mem=4GB,ncpus=2,jobfs=1GB,other=gdata',
            '-l', 'wd',
            '-N', 'sync-{}'.format(job_name),
            '-m', 'a',
            *qsub_opts,
            '-e', str(error_file),
            '-o', str(output_file),
            '-W', ','.join(attributes),
            '--',
            *sync_command
        ]

        click.echo(' '.join(command))
        output = check_output(command)
        job_id = output.decode('utf-8').strip(' \\n')
        return job_id


@click.command()
@click.argument('job_name')
@click.argument('tile_folder', type=click.Path(exists=True, readable=True, writable=False))
@click.option('--dry-run', is_flag=True, default=False)
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--project', '-P', default='v10')
@click.option('--run-folder',
              type=click.Path(exists=True, readable=True, writable=True),
              # 'cache' folder in current directory.
              default='runs')
@click.option('--submit-limit', type=int, default=None, help="Max number of jobs to submit (remaining tiles will "
                                                             "not be submitted)")
@click.option('--concurrent-jobs', type=int, default=12, help="Number of PBS jobs to run concurrently")
def main(job_name: str,
         tile_folder: str,
         run_folder: str,
         submit_limit: int,
         concurrent_jobs: int,
         dry_run: bool,
         queue: str,
         project: str):
    tile_path = Path(tile_folder).absolute()
    run_path = Path(run_folder).absolute()

    with index_connect(application_name='sync-' + job_name) as index:
        collections.init_nci_collections(AgdcDatasetPathIndex(index))

        submitter = SyncSubmission(run_path.joinpath('cache'), project, queue, dry_run, verbose=True, workers=4)

        _find_and_submit(job_name, tile_path, run_path, concurrent_jobs, submit_limit, submitter)

# Validate scenes:
def _find_and_submit(job_name: str,
                     tile_path: Path,
                     run_path: Path,
                     concurrent_jobs: int,
                     submit_limit: int,
                     submitter: SyncSubmission):
    submitter.warm_cache(tile_path)

    submitted = 0
    # To maintain concurrent_jobs limit, we set a pbs dependency on previous jobs.
    # mapping of concurrent slot number to the last job id to be submitted in it.
    # type: Mapping[int, str]
    last_job_slots = {}
    for task_name, input_folders in make_tile_jobs(tile_path):

        subjob_name = '{}{}'.format(job_name, task_name)
        if submitted == submit_limit:
            click.echo("Submit limit ({}) reached, done.".format(submit_limit))
            break

        require_job_id = last_job_slots.get(submitted % concurrent_jobs)

        subjob_run_path = run_path.joinpath(job_name, subjob_name)
        fileutils.mkdir_p(subjob_run_path)

        output_path = subjob_run_path.joinpath('out.tsv')
        if output_path.exists():
            click.echo("{}: output exists, skipping".format(subjob_name))
            continue

        job_id = submitter.submit(
            # Folders are named "X_Y", we glob for all folders with the give X coord.
            input_folders=list(input_folders),
            output_file=output_path,
            error_file=subjob_run_path.joinpath('err.log'),
            job_name=subjob_name,
            require_job_id=require_job_id,
        )

        if job_id:
            last_job_slots[submitted % concurrent_jobs] = job_id
            submitted += 1
            click.echo("[{}] {}: submitted {}".format(submitted, subjob_name, job_id))

        time.sleep(SUBMIT_THROTTLE_SECS)


def find_tile_xs(tile_path):
    """
    For input tile_path, get list of unique tile X values.

    Inner folders are named "X_Y", eg "-12_23"
    """
    tile_xs = set(int(p.name.split('_')[0]) for p in tile_path.iterdir() if p.name != 'ncml')
    tile_xs = sorted(tile_xs)
    click.echo("Found %s total jobs" % len(tile_xs))
    return tile_xs


def make_tile_jobs(tile_path) -> Iterable[Tuple[str, Iterable[Path]]]:
    """
    Tries to yield one job per x value (tiles are X_Y), but if the number of files
    is above FILES_PER_JOB_CUTOFF it will split it up.

    This could be much simpler if it ignored X entirely and just grouped X_Ys up to file count, but
    our old ones were grouped purely by X, and this maintains backwards compat so that the already-completed
    X-folders aren't rerun).
    """
    for tile_x in find_tile_xs(tile_path):
        tile_x_ys = tile_path.glob('{}_*'.format(tile_x))

        task_number = 1
        input_paths = []
        input_paths_file_count = 0

        for tile_x_y in tile_x_ys:
            input_paths.append(tile_x_y)
            input_paths_file_count += sum(1 for _ in tile_x_y.rglob('*.nc'))

            # If this pushed us over the cutoff, yield a task with the folders so far.
            if input_paths_file_count > FILES_PER_JOB_CUTOFF:
                yield "{:+04d}-{}".format(tile_x, task_number), input_paths
                task_number += 1
                input_paths = []
                input_paths_file_count = 0

        if input_paths:
            yield "{:+04d}-{}".format(tile_x, task_number), input_paths


def get_collection(tile_path: Path) -> collections.Collection:
    """
    Get the collection that covers the given path
    """
    cs = list(collections.get_collections_in_path(tile_path))
    if not cs:
        raise click.UsageError("No collections found for path {}".format(tile_path))
    if len(cs) > 1:
        raise click.UsageError("Multiple collections found for path: too broad? {}".format(tile_path))
    collection = cs[0]
    return collection


if __name__ == '__main__':
    # Eg. scripts/submit-tile-sync.py 5fc /g/data/fk4/datacube/002/LS5_TM_FC
    main()
