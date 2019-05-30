from setuptools import setup, find_packages

with open("README", 'r') as f:
    long_description = f.read()

setup(
   name='fkwan',
   version='1.0',
   description='Package with functions',
   license="MIT",
   long_description=long_description,
   author='Fu Ern Kwan',
   author_email='kwanern@umich.edu',
   url="http://www.foopackage.com/",
   packages=find_packages(),  #same as name
   install_requires=[
       'spark',
       'pyspark',
       'sklea'
   ], #external packages as dependencies
)