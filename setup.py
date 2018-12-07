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
        'wikia-common-kibana==2.2.6',
        'sql_metadata==1.2',
    ],
    extras_require={
        'dev': [
            'coverage==4.5.2',
            'pylint>=1.9.2, <=2.1.1',  # 2.x branch is for Python 3
            'pytest==4.0.0',
        ]
    },
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'query_digest=scripts.query_digest:main'
        ],
    }
)
