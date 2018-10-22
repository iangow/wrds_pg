import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="wrdstopg",
    version="0.0.9",
    author="Ian Gow",
    author_email="ian.gow@unimelb.edu.au",
    description="Download wrds tables and upload to PostgreSQL, upload SAS file to PG",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/iangow-public/wrds_pg/",
    packages=setuptools.find_packages(),
    install_requires=['pandas','sqlalchemy', 'paramiko'],
    python_requires=">=3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
