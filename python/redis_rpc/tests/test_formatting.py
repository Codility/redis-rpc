import glob
import os.path
import yapf

PACKAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')


def test_formatting():
    files = glob.glob("%s/**/*.py" % PACKAGE_DIR, recursive=True)
    assert 0 == yapf.main(['arg0-placeholder', '--diff'] + files)
