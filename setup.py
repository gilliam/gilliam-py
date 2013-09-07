from setuptools import setup, find_packages

setup(
    name='gilliam-py',
    version='0.0',
    packages=find_packages(),
    include_package_data=True,
    author='Johan Rydberg',
    author_email='johan.rydberg@gmail.com',
    url='https://github.com/gilliam/gilliam-py',
    description='Python bindings for the Gilliam platform',
    long_description=None,
    license='Apache 2.0',
    install_requires=['requests', 'python-circuit']
)
