import logging

from os import getenv

logging.basicConfig(
    level=logging.DEBUG if getenv('DEBUG') == '1' else logging.INFO,
    format='%(asctime)s %(name)-35s %(levelname)-8s %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)
