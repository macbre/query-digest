from setuptools import setup

from digest import __version__

setup(
    name='query-digest',
    version=__version__,
    description='Reports queries issued by a given MediaWiki feature',
    url='https://github.com/Wikia/sus-dynks/query-digest',
    author='macbre',
    author_email='macbre@wikia-inc.com',
    install_requires=[
        'tabulate==0.7.7',
        'wikia.common.kibana==1.2.1',
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'query_digest=scripts.query_digest:main'
        ],
    }
)
