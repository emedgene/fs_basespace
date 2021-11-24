import pytest
import os


if __name__ == '__main__':
    os.environ['EMG_ENV'] = 'tests'
    pytest.main(['--cov-config', '.coveragerc', '--cov',
                 './', '--cov-report', 'xml:tests-reports/coverage/cobertura.xml',
                 '--junitxml=tests-reports/junit/junit.xml'])
