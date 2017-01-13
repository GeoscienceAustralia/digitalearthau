

from subprocess import call


class MDSSClient(object):
    def __init__(self, project):
        self.project = project

    def _call(self, *args):
        return call(('mdss', '-P', self.project, *args))

    def put(self, source_paths, dest_path):
        retcode = self._call('put', *source_paths, dest_path)
        if retcode == -1:
            raise RuntimeError("Failed to transfer to {} MDSS: {} -> {}".format(self.project, source_paths, dest_path))

    def make_dirs(self, path):
        if self._call('ls', '-d', path) != 0:
            if self._call('mkdir', path) != 0:
                raise RuntimeError("Failed to mkdir on MDSS {} at {}".format(self.project, path))

    def to_uri(self, path):
        return 'mdss://{project}/{offset}'.format(
            project=self.project,
            offset=path,
        )
