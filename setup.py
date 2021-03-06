from setuptools import setup, find_packages

setup(
  name='senseis',
  version='0.0.1',
  description='Coinbase Pro Data Extraction',
  authors='senseis',
  author_emails='senseisworkspace@gmail.com',
  url='https://github.com/armandli/cbp-book-extraction',
  packages=find_packages(exclude=['senseis']),
  package_data={},
  data_files={},
  install_requires=[
    'requests',
    'pytz',
    's3cmd',
  ],
  entry_points={
    'console_scripts':[
        'candle_extractor = senseis.extraction.candle_extractor:main'
    ]
  },
  scripts=[]
)
