from setuptools import setup, find_packages

with open("README", 'r') as f:
    long_description = f.read()

setup(
   name='fkwan',
   version='1.00',
   description='Package with functions',
   license="MIT",
   long_description=long_description,
   author='Fu Ern Kwan',
   author_email='kwanern@umich.edu',
   packages=find_packages(),  #same as name
)
