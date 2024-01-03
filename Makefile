clean:
	rm -rf dist build *.egg-info
	find . -name ".pyc" -delete

upload: clean
	python setup.py sdist bdist_wheel
	twine upload dist/*

LENGTH=120


.PHONY: pylint
pylint:
	pylint cashews --reports=n --max-line-length=$(LENGTH)
