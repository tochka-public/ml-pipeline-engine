from os.path import dirname, join

import setuptools

from version import version

install_requires = [
    'networkx==3.1',
    'python-ulid==1.1.0',
    'cloudpickle==2.2.1',
]
tests_requires = [
    'pytest>=4.0.0',
    'pytest-mock>=3.10.0',
    'pre-commit>=2.0.0',
    'flake8>=3.8.3',
    'pytest-cov>=2.11.0',
    'pytest-asyncio==0.18.3',
]

with open(join(dirname(__file__), 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name='ml-pipeline-engine',
    version=version,
    packages=setuptools.find_packages(),
    description='Фреймворк для работы с пайплайном ML моделей',
    long_description=long_description,
    license='MIT',
    install_requires=install_requires,
    tests_require=tests_requires,
    extras_require={'tests': [tests_requires]},
    setup_requires=['wheel'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
