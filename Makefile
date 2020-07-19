clean:
	rm -rf dist build *.egg-info
	find . -name ".pyc" -delete

upload: clean
	python setup.py sdist bdist_wheel
	twine upload -u krukov dist/*

LENGTH=120

.PHONY: format
format: black isort

.PHONY: pylint
pylint:
	pylint cashews --reports=n --max-line-length=$(LENGTH)

.PHONY: isort
isort:
	@echo -n "Run isort"
	isort --lines $(LENGTH) -rc cashews tests

.PHONY: black
black:
	@echo -n "Run black"
	black -l $(LENGTH) cashews tests examples

.PHONY: check-isort
check-isort:
	isort --lines $(LENGTH) -vb -rc --check-only -df cashews tests

.PHONY: check-styles
check-styles:
	pycodestyle cashews tests --max-line-length=$(LENGTH) --format pylint

.PHONY: check-black
check-black:
	black --check --diff -v -l $(LENGTH) cashews tests
