from setuptools import setup, find_packages

import versioneer
versioneer.versionfile_source = "gilliam/_version.py"
versioneer.versionfile_build = "gilliam/_version.py"
versioneer.tag_prefix = ""
versioneer.parentdir_prefix = ""
commands = versioneer.get_cmdclass().copy()

setup(
    name='gilliam-py',
    version=versioneer.get_version(),
    packages=find_packages(),
    include_package_data=True,
    author='Johan Rydberg',
    author_email='johan.rydberg@gmail.com',
    url='https://github.com/gilliam/gilliam-py',
    description='Python bindings for the Gilliam platform',
    long_description=None,
    license='Apache 2.0',
    cmdclass=commands,
    install_requires=[
        'requests >= 2.0',
        'python-circuit'
        ]
)
