from setuptools import setup, find_packages # type: ignore

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="hvpdb",
    version="1.0.0",
    description="HVPDB: Secure Embedded NoSQL Database with ACID Transactions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="8w6s",
    url="https://github.com/8w6s/hvpdb",
    packages=find_packages(),
    install_requires=[
        "cryptography>=41.0.0",
        "msgpack>=1.0.5",
        "argon2-cffi>=21.3.0",
        "rich>=13.0.0",
        "typer>=0.9.0",
        "zstandard>=0.21.0",
        "portalocker>=2.7.0",
    ],
    entry_points={
        'console_scripts': [
            'hvpdb=hvpdb.cli:app',
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)