from setuptools import setup, find_packages

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
   name='fkwan',
   version='1.00',
   description='Package with functions',
   license="MIT",
   long_description=long_description,
   long_description_content_type="text/markdown",
   author='Fu Ern Kwan',
   author_email='kwanern@umich.edu',
   url = "https://github.com/kwanern/fkwan.git"
)
