from setuptools import setup, find_packages


with open("Readme.md", "rt", encoding="utf8") as f:
    readme = f.read()

PROJECT_NAME = "cashews"
VERSION = "0.12.0"

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
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Framework :: AsyncIO",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "Operating System :: OS Independent",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development",
        "Typing :: Typed",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
    ],
    packages=find_packages(),
    install_requires=None,
    extras_require={
        "redis": ["aioredis>=1.0.0"],
        "dev": ["black", "codecov", "coverage", "flake8", "pytest", "isort", "pylint", "pytest-asyncio"],
    },
)
