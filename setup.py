#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages

setup(
    name='pydubbo',
    version=1,
    description=(
        '用python调用dubbo可以用于测试等'
    ),
    long_description=open('README.rst').read(),
    author='zhangliming',
    author_email='149151874@qq.com',
    maintainer='zhangliming',
    maintainer_email='zhangliming',
    license='BSD License',
    packages=find_packages(),
    platforms=["all"],
    url='https://github.com/morningzhang/dubbo',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries'
    ],
)