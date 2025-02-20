import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="das_mock",
    version="0.1.0",
    author="RAW Labs",
    author_email="hello@raw-labs.com",
    description="A mock DAS Python server",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/raw-labs/das-server-python-mock",
    packages=setuptools.find_packages(include=["com.*", "das_mock.*"]),
    include_package_data=True,
    install_requires=[
        "grpcio==1.51.1",
        "grpcio-tools==1.51.1",
        "protobuf==4.21.12",
        "PyYAML==6.0",
        "requests>=2.31.0",
        "urllib3<2.0.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "dsa-mock=das_mock.main:main",  # e.g. a CLI
        ],
    },
)
