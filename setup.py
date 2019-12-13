from setuptools import setup, find_packages


with open("README.rst", "rt", encoding="utf8") as f:
    readme = f.read()

PROJECT_NAME = "cashews"
VERSION = "0.1.0"

setup(
    name=PROJECT_NAME,
    version=VERSION,
    author="Dmitry Kryukov",
    url="https://github.com/Krukov/" + PROJECT_NAME,
    download_url="https://github.com/Krukov/" + PROJECT_NAME + "/tarball/" + VERSION,
    author_email="glebov.ru@gmail.com",
    description="cache tools with async power",
    keywords="cache aio async multicache aiocache",
    long_description=readme,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Framework :: AsyncIO",
    ],
    packages=find_packages(),
    install_requires=None,
    extras_require={
        "redis": ["aioredis>=1.0.0"],
        "dev": ['black;python_version>="3.6"', "codecov", "coverage", "flake8", "pytest", "pylint" "pytest-asyncio"],
    },
)
