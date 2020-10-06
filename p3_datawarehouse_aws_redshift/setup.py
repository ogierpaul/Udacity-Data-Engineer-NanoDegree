from setuptools import setup, find_packages

setup(name='p3src',
      version='0.1',
      url='https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree',
      packages=find_packages(),
      install_requires=[
          'boto3',
          'pandas',
          'psycopg2'
      ]
)
