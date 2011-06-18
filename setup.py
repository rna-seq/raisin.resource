from setuptools import setup, find_packages
import sys, os

version = '1.0'

setup(name='big.resource',
      version=version,
      description="",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='',
      author_email='',
      url='',
      license='',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      package_data = {'big.resource':['*.ini']},
      namespace_packages = ['big'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
          'configobj',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
