from setuptools import find_packages
from setuptools import setup

install_requires = [
    "python-dateutil",
    "pandas",
    "requests"
]

setup(
    name="pbftstats",
    version="0.0.0",
    description="Transaction signer stats for test pbft",
    author="OnedgeLee",
    author_email="Onedge.Lee@gmail.com",
    packages=find_packages(),
    install_requires=install_requires,
)
