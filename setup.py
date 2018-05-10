from setuptools import setup

from digest import __version__

setup(
    name='query-digest',
    version=__version__,
    description='Reports queries issued by a given MediaWiki feature / Pandora service / Backend script',
    url='https://github.com/macbre/query-digest',
    author='Maciej Brencz',
    author_email='maciej.brencz@gmail.com',
    install_requires=[
        'docopt==0.6.2',
        'tabulate==0.8.2',
        'wikia-common-kibana==2.2.5',
        'sql_metadata==1.1.2',
    ],
    extras_require={
        'dev': [
            'coverage==4.5.1',
            'pylint==1.8.4',
            'pytest==3.5.1',
        ]
    },
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'query_digest=scripts.query_digest:main'
        ],
    }
)
