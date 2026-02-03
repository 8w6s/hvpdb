from setuptools import setup, find_packages
with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='hvpdb',
    version='1.0.3',
    description='High Velocity Python Database (NoSQL, Embedded, Encrypted)',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='HVPDB Bot',
    author_email='bot@hvpdb.io',
    packages=find_packages(),
    install_requires=[
        'cryptography>=41.0.0',
        'msgpack>=1.0.5',
        'argon2-cffi>=21.3.0',
        'rich>=13.0.0',
        'typer>=0.9.0',
        'zstandard>=0.21.0',
        'portalocker>=2.7.0',
        'pydantic>=2.0.0',
        'prompt_toolkit>=3.0.0'
    ],
    extras_require={
        'cli': ['typer>=0.9.0', 'rich>=13.0.0', 'prompt_toolkit>=3.0.0'],
        'server': ['fastapi>=0.100.0', 'uvicorn>=0.22.0'],
    },
    entry_points={
        'console_scripts': [
            'hvpdb=hvpdb.cli:app'
        ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
        'Topic :: Database :: Database Engines/Servers'
    ],
    python_requires='>=3.8'
)