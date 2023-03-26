from setuptools import setup

setup(
    name='aerology-influxdb-api',
    version='0.3.0',
    author='Can Isik, Yamac Isik',
    author_email='can@fold.ai',
    package_dir={'aerology_influxdb_api': 'src/aerology_influxdb_api'},
    packages=['aerology_influxdb_api'],
    description='package that can be used as the api to push/pull data from aerology influxdb',
    install_requires=[
        'influxdb-client',
        'jsonschema',
        'pandas',
    ],
)
