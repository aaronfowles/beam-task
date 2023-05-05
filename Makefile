install:
	pip install --quiet -r requirements.txt

test: install
	python3 -m unittest discover tests

run: install
	python3 -m pipeline