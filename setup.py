import setuptools
from pyesbulk import __VERSION__

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyesbulk",
    version=__VERSION__,
    author="Peter A. Portante",
    author_email="peter.a.portante@gmail.com",
    description="An opinionated Elasticsearch bulk indexer for Python.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/distributed-system-analysis/py-es-bulk",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)
