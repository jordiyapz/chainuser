import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="chainuser",
    version="0.1.0",
    author="Jordi Yaputra",
    author_email="jordiyaputra@gmail.com",
    description=(""),
    license="MIT",
    packages=['src'],
    long_description=read('README.md'),
)
