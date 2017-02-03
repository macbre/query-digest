install:
	pip install -e .

tsv:
	WIKIA_DOCROOT=~/app get_tsv | tee ../02_code_quality.tsv
