#!/usr/bin/env python

from distutils.core import setup

setup(name='openbmp-file-consumer',
      version='0.1.0',
      description='Basic openbmp file consumer',
      author='Tim Evens',
      author_email='tim@openbmp.org',
      url='',
      data_files=[('etc', ['src/etc/openbmp-file-consumer.yml'])],
      package_dir={'': 'src/site-packages'},
      packages=['openbmp', 'openbmp.parsed'],
      scripts=['src/bin/openbmp-file-consumer']
     )
