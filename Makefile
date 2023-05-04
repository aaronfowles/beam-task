install:
	pip install --quiet -r requirements.txt

test: install
	pytest

run: install
	python3 -m main