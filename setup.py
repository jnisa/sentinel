# Project setup script

from setuptools import setup, find_packages

# Setup the package
setup(
    name='sentinel',
    version='0.0.1',
    description='Package to grant observability to the pipelines running on a platform.',
    author='Data Team',
    include_package_data=True,
    zip_safe=False
)