from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def get_this_module():
    # TODO: distinguish different instances?
    return 'dea-prod/{}'.format(__version__)
