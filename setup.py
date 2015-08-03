#!/usr/bin/env python
# coding: utf-8
from distutils.core import setup
import io

description = io.open('README').read()

setup(
    name='omlite',
    version='0.1.0',

    author='Krisztián Fekete',
    author_email='fekete.krisztyan@gmail.com',
    description='Object Mapper for SQLite',
    long_description=description,
    url='https://github.com/krisztianfekete/omlite',

    keywords='sqlite object mapper',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'License :: Public Domain',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
    license='Unlicense',

    py_modules=['omlite'],
)
