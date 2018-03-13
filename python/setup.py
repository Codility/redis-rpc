from setuptools import setup


setup(
    name='redis-rpc',
    packages=['redis_rpc'],
    version='0.1',
    description='Minimalistic rpc-over-redis library',
    author='Marcin Kaszynski',
    author_email='marcink@codility.com',
    url='https://github.com/Codility/redis-rpc',
    setup_requires=['pytest-runner'],
    install_requires=['redis'],
    tests_require=['pytest', 'pytest-redis', 'pytest-timeout'],
    python_requires=">=3.5"
)
