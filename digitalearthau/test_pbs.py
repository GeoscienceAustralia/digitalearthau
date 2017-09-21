from . import pbs


def test_is_under_pbs():
    assert pbs.is_under_pbs() is False


def test_pbs_hostname():
    assert isinstance(pbs.hostname(), str)


def test_env():
    ee = pbs.get_env()
    assert isinstance(ee, dict)

    ss = pbs.generate_env_header()
    assert isinstance(ss, str)
    assert len(ss) > 0

    ss = pbs.generate_env_header(CUSTOM_ENV_334455='foo')
    assert ss.find('CUSTOM_ENV_334455') >= 0


def test_script():
    script = '''
echo 'yay!*;'
pwd
    '''

    w_script = pbs.wrap_script(script)
    assert isinstance(w_script, str)

    proc = pbs.pbsdsh(0, script, test_mode=True)
    assert proc.wait() == 0
    assert proc.stdout.readline().decode('utf8') == 'yay!*;\n'

    proc = pbs.pbsdsh(0, 'exit 10', test_mode=True)
    assert proc.wait() == 10
