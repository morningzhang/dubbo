#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages

setup(
    name='pydubbo',
    version='0.0.9',
    description=(
        'python调用dubbo,目前支持dubbo协议，不需要服务端修改成jsonrpc.实现客户端的负载均衡、配合Zookeeper自动发现服务功能等'
    ),
    install_requires=[
            "kazoo>=2.5.0",
            "bitstring>=3.1.5",
            "python_hessian>=1.0.2"
        ],
    long_description=open('README.rst').read(),
    author='zhangliming',
    author_email='149151874@qq.com',
    maintainer='zhangliming',
    maintainer_email='149151874@qq.com',
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