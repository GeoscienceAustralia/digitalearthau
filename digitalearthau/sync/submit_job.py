#!/usr/bin/env python
"""
Submit sync PBS jobs.

The task_split function is currently specific to tiles, but can be generalised

Example usage: submit_job.py 5fc /g/data/fk4/datacube/002/LS5_TM_FC

5fc is just the name for the job: subsequent resubmissions will not rerun jobs with the same name
if output files exist.

A run folder is used (defaulting to `runs` in current dir) for output.
"""
import datetime
import logging
import os
import time
from collections import defaultdict
from pathlib import Path
from subprocess import check_output
from typing import Mapping, List, Optional, Tuple, Iterable

import click
import typing

from boltons import fileutils
from click import style

from datacube.index import index_connect
from digitalearthau import collections
from digitalearthau.index import AgdcDatasetPathIndex
from digitalearthau.paths import get_dataset_paths
from digitalearthau.sync import scan

SUBMIT_THROTTLE_SECS = 1

FILES_PER_JOB_CUTOFF = 15000

_LOG = logging.getLogger(__name__)

DEFAULT_WORK_FOLDER = '/g/data/v10/work/sync/{collection.name}/{work_time:%Y-%m-%dT%H%M}'
DEFAULT_CACHE_FOLDER = '/g/data/v10/work/sync/{collection.name}/cache'

# All tasks submitted during this session will have the same work timestamp
TASK_TIME = datetime.datetime.now()


class Task:
    # A task has a list of paths from a single collection.
    def __init__(self, input_paths: List[Path], dataset_count: int) -> None:
        self.input_paths = input_paths
        self.dataset_count = dataset_count

        if not input_paths:
            raise ValueError("Minimum of one input path in a task")

        # Sanity check: all provided input paths should resolve to the same single collection
        all_collections = set(get_collection(input_path) for input_path in input_paths)
        if not len(all_collections) == 1:
            raise ValueError("A task path should match exactly one collection. Got %r" % (collections,))

        self._collection = all_collections.pop()

    @property
    def collection(self):
        return self._collection

    def resolve_path(self, pattern: str) -> Path:
        return Path(pattern.format(
            collection=self.collection,
            work_time=TASK_TIME
        ))

    def __repr__(self) -> str:
        return '%s(%r, %r)' % (
            self.__class__.__name__,
            self.input_paths,
            self.dataset_count
        )


class SyncSubmission(object):
    def __init__(self, cache_folder: str, project='v10', queue='normal', dry_run=False, verbose=True,
                 workers=4) -> None:
        self.project = project
        self.queue = queue
        self.dry_run = dry_run
        self.verbose = verbose
        self.workers = workers
        self.cache_folder = cache_folder

    def warm_cache(self, tasks: Iterable[Task]):
        # Update the cached path list ahead of time, so PBS jobs don't waste time doing it themselves.
        click.echo("Checking path list, this may take a few minutes...")

        done_collections = set()

        for task in tasks:
            if task.collection in done_collections:
                continue
            cache_path = Path(task.resolve_path(self.cache_folder))
            scan.build_pathset(task.collection, cache_path=cache_path)

            done_collections.add(task.collection)

    def submit(self,
               task: Task,
               output_file: Path,
               error_file: Path,
               job_name: str,
               require_job_id: Optional[int]) -> str:

        # Output files readable by others.
        attributes = ['umask=33']

        sync_opts = []
        if require_job_id:
            attributes.extend(['depend=afterany:{}'.format(str(require_job_id).strip())])
        if self.verbose:
            sync_opts.append('-v')
        if not self.dry_run:
            # Defaults. Trash things archived a while ago, and update the index's locations to match disk.
            sync_opts.extend(['--trash-archived', '--update-locations'])

            # Do we trust the index or disk when there are unknown files?
            if task.collection.trust:
                # For tile products like the current FC we trust the index over the filesystem.
                # (jobs that failed part-way-through left datasets on disk and were not indexed)

                # Scene products are the opposite:
                # Only complete scenes are written to fs, so '--index-missing' instead of trash.

                trust_options = {
                    'disk': ['--index-missing'],
                    'index': ['--trash-missing'],
                }
                if task.collection.trust not in trust_options:
                    raise RuntimeError("Unknown trust type %r", task.collection.trust)
                sync_opts.extend(trust_options[task.collection.trust])

        sync_command = [
            'python', '-m', 'digitalearthau.sync',
            '-j', str(self.workers),
            '--cache-folder', str(task.resolve_path(self.cache_folder)),
            *sync_opts,
            *(map(str, task.input_paths))
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
        job_id = output.decode('utf-8').strip(' \n')
        return job_id


@click.command()
@click.argument('folders',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
@click.option('--dry-run', is_flag=True, default=False)
@click.option('--queue', '-q',
              default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--project', '-P',
              default='v10')
@click.option('--work-folder',
              type=click.Path(readable=True, writable=True),
              default=DEFAULT_WORK_FOLDER)
@click.option('--cache-folder',
              type=click.Path(readable=True, writable=True),
              default=DEFAULT_CACHE_FOLDER)
@click.option('--max-jobs',
              type=int,
              default=50,
              help="Maximum number of PBS jobs to allow (paths will be grouped to get under this limit)")
@click.option('--concurrent-jobs',
              type=int,
              default=12,
              help="Number of PBS jobs to allow to *run* concurrently. The latter jobs will be submitted "
                   "with run dependencies on earlier ones.")
@click.option('--submit-limit',
              type=int,
              default=None,
              help="Stop submitting after this many jobs. Useful for testing.")
def main(folders: Iterable[str],
         dry_run: bool,
         queue: str,
         project: str,
         work_folder: str,
         cache_folder: str,
         max_jobs: int,
         concurrent_jobs: int,
         submit_limit: int):
    input_paths = [Path(folder).absolute() for folder in folders]

    with index_connect(application_name='sync-submit') as index:
        collections.init_nci_collections(AgdcDatasetPathIndex(index))
        submitter = SyncSubmission(cache_folder, project, queue, dry_run, verbose=True, workers=4)
        click.echo(
            "{} input path(s)".format(len(input_paths))
        )
        tasks = _paths_to_tasks(input_paths)
        click.echo(
            "Found {} tasks across collection(s): {}".format(
                len(tasks),
                ', '.join(set(t.collection.name for t in tasks))
            )
        )

        if len(tasks) > max_jobs:
            click.echo(
                "Grouping (max_jobs={})".format(max_jobs)
            )
        tasks = group_tasks(tasks, maximum=max_jobs)

        total_datasets = sum(t.dataset_count for t in tasks)
        click.secho(
            "Submitting {} total jobs with {} datasets (avg {:.2f} each)...".format(
                len(tasks),
                total_datasets,
                total_datasets / len(tasks)
            ),
            bold=True
        )

        _find_and_submit(tasks, work_folder, concurrent_jobs, submit_limit, submitter)


def _paths_to_tasks(input_paths: List[Path]) -> List[Task]:
    # Remove duplicates
    normalised_input_paths = set(p.absolute() for p in input_paths)

    def dataset_folder_path(dataset_path):
        # Get their dataset's parent folders: (typically the "x_y" for tiles, the month for scenes)

        # Get the base path for the dataset.
        # eg. "LS8_SOME_SCENE_1/ga-metadata.yaml" to "LS8_SOME_SCENE_1"
        #  or "LS7_SOME_TILE.nc" to itself
        base_path, _ = get_dataset_paths(dataset_path)

        return base_path.parent

    parent_folder_counts = uniq_counts(dataset_folder_path(dataset_path)
                                       for input_path in normalised_input_paths
                                       for collection in collections.get_collections_in_path(input_path)
                                       for dataset_path in collection.iter_fs_paths_within(input_path))

    # Sanity check: Each of these parent folders should still be within an input path
    for path, count in parent_folder_counts:
        if not any(str(path).startswith(str(input_path))
                   for input_path in normalised_input_paths):
            raise NotImplementedError("Giving a specific dataset rather than a folder of datasets?")

    return [Task([p], c) for p, c in parent_folder_counts]


def group_tasks(tasks: List[Task], maximum) -> List[Task]:
    """
    >>> collections._add(collections.Collection('test', {}, ['/test/*'], ()))
    >>> two = [Task(['/test/a', '/test/b'], 3), Task(['/test/c'], 2)]
    >>> group_tasks(two, maximum=2)
    [Task(['/test/a', '/test/b'], 3), Task(['/test/c'], 2)]
    >>> group_tasks(two, maximum=1)
    [Task(['/test/a', '/test/b', '/test/c'], 5)]
    """
    # Combine the two smallest repeatedly until under the limit.
    while len(tasks) > maximum:
        # Ordered: most datasets to least
        tasks.sort(key=lambda s: s.dataset_count, reverse=True)

        max_count = tasks[-1].dataset_count
        if max_count > FILES_PER_JOB_CUTOFF:
            _LOG.warning('Unusually large number of datasets in a single folder: %s', max_count)

        a = tasks.pop()
        b = tasks.pop()
        tasks.append(
            Task(input_paths=sorted(a.input_paths + b.input_paths),
                 dataset_count=a.dataset_count + b.dataset_count)
        )

    return tasks


T = typing.TypeVar('T')


def uniq_counts(paths: Iterable[T]) -> List[Tuple[T, int]]:
    """
    Group unique items, retuning them with a count of instances.

    >>> uniq_counts([])
    []
    >>> uniq_counts(['a'])
    [('a', 1)]
    >>> uniq_counts(['a', 'b', 'b'])
    [('a', 1), ('b', 2)]
    """
    s = defaultdict(int)
    for p in paths:
        s[p] += 1
    return list(sorted(s.items(), key=lambda t: t[1]))


def _find_and_submit(tasks: List[Task],
                     work_folder: str,
                     concurrent_jobs: int,
                     submit_limit: int,
                     submitter: SyncSubmission):
    submitter.warm_cache(tasks)

    submitted = 0
    # To maintain concurrent_jobs limit, we set a pbs dependency on previous jobs.
    # mapping of concurrent slot number to the last job id to be submitted in it.
    # type: Mapping[int, str]
    last_job_slots = {}

    for task in tasks:
        if submitted == submit_limit:
            click.echo("Submit limit ({}) reached, done.".format(submit_limit))
            break

        require_job_id = last_job_slots.get(submitted % concurrent_jobs)

        run_path = task.resolve_path(work_folder).joinpath('{:03d}'.format(submitted))
        if run_path.exists():
            raise RuntimeError("Calculated job folder should be unique? Got %r" % (run_path,))

        fileutils.mkdir_p(run_path)

        # Not used by the job, but useful for our reference; to know what has been submitted already.
        run_path.joinpath('inputs.txt').write_text('\n'.join(map(str, task.input_paths)) + '\n')

        job_id = submitter.submit(
            task=task,
            output_file=(run_path.joinpath('out.tsv')),
            error_file=run_path.joinpath('err.log'),
            job_name='{}-{:02}'.format(task.collection.name, submitted),
            require_job_id=require_job_id,
        )

        if job_id:
            run_path.joinpath('job.txt').write_text('{}\n'.format(job_id))

            last_job_slots[submitted % concurrent_jobs] = job_id
            submitted += 1

            click.echo(
                "{prefix}: submitted {job_id} with {dataset_count} datasets using directory {run_path}".format(
                    prefix=style(
                        "[{:02d} {}]".format(submitted, task.collection.name),
                        fg='blue', bold=True
                    ),
                    job_id=style(job_id, bold=True),
                    dataset_count=style(str(task.dataset_count), bold=True),
                    run_path=style(str(run_path), bold=True)
                )
            )

        time.sleep(SUBMIT_THROTTLE_SECS)


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
    main()
