from . import qsub


def test_parse_args():
    p = qsub.parse_comma_args('nodes=1,mem=small')
    assert 'mem' in p
    assert 'nodes' in p
    assert p['mem'] == 'small'
    assert p['nodes'] == '1'


def test_norm_qsub_params():
    p = qsub.parse_comma_args('nodes=1,mem=small,walltime=10s')
    p = qsub.norm_qsub_params(p)

    assert p['ncpus'] == 16
    assert p['walltime'] == '0:00:10'
    assert p['mem'] == '32256MB'

    p = qsub.parse_comma_args('nodes=1,mem=small,walltime=10m')
    p = qsub.norm_qsub_params(p)

    assert p['ncpus'] == 16
    assert p['walltime'] == '0:10:00'
    assert p['mem'] == '32256MB'

    p = qsub.parse_comma_args('ncpus=1, mem=medium, walltime=3h')
    p = qsub.norm_qsub_params(p)

    assert p['ncpus'] == 1
    assert p['walltime'] == '3:00:00'
    assert p['mem'] == '4096MB'


def test_remove_args():
    args1 = '--qsub 10 --foo bar'.split(' ')
    args2 = '--qsub=10 --foo bar'.split(' ')
    args3 = '--removed --foo bar'.split(' ')

    assert qsub.remove_args('--qsub', args1, 1) == ['--foo', 'bar']
    assert qsub.remove_args('--qsub', args2, 1) == ['--foo', 'bar']
    assert qsub.remove_args('--removed', args3, 0) == ['--foo', 'bar']
