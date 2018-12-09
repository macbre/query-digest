coverage_options = --include='digest/*,scripts/*' --omit='test/*'

install:
	pip install -e .[dev]

test:
	pytest -v

coverage:
	rm -f .coverage*
	rm -rf htmlcov/*
	coverage run -p -m pytest -v
	coverage combine
	coverage html -d htmlcov $(coverage_options)
	coverage xml -i
	coverage report $(coverage_options)

lint:
	pylint digest/ scripts/

.PHONY: test
