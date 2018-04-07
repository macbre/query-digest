install:
	pip install -e .
	pip install -e .[dev]

test:
	pytest tests/
