#!/usr/bin/env python

from setuptools import setup, find_packages

install_requires = []

tests_require = ["pytest"]

with open("README.md", "r") as fh:
    long_description = fh.read()

VERSION = "0.1.0"
setup(
    name="coinbasepro_fix_client",
    version=VERSION,
    author="Carl Colena",
    author_email="carl.colena@gmail.com",
    license="MIT",
    url="https://github.com/Cartoonman/coinbase-pro-fix-client-python",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={"tests": tests_require},
    package_data={"coinbasepro_fix_client": ["fix_protocol/*"]},
    description="The unofficial Python client for the Coinbase Pro API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    download_url=f"https://github.com/Cartoonman/coinbase-pro-fix-client-python/archive/v{VERSION}.zip",
    keywords=[
        "fix-client",
        "trade",
        "bitcoin",
        "ethereum",
        "BTC",
        "ETH",
        "client",
        "fix",
        "exchange",
        "crypto",
        "currency",
        "trading",
        "trading-api",
        "coinbase",
        "pro",
        "prime",
        "coinbasepro",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Information Technology",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)
