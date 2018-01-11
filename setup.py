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
        'tabulate==0.7.7',
        # 'wikia.common.kibana==2.1.2',
        "elasticsearch>=5.0.0,<6.0.0",
        "python-dateutil==2.2",
        'pytest==3.1.2',
        'sql_metadata==1.0',
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'query_digest=scripts.query_digest:main'
        ],
    }
)
