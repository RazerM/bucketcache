import subprocess

from shovel import task


@task
def test():
    subprocess.call(
        ['py.test', 'bucketcache/', '--run-slow', '-n', '4', '-rs'])


@task
def coverage():
    subprocess.call(
        ['py.test', 'bucketcache/', '--run-slow', '-n', '4', '--cov',
         'bucketcache', '--cov-report', 'html', '-rs', '--cov-config',
         '.coveragerc'])


@task
def benchmark():
    subprocess.call(['py.test', 'bucketcache/', '--run-slow',
                     '--benchmark-only'])
