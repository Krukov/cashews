clean:
	rm -rf dist build *.egg-info
	find . -name ".pyc" -delete

upload: clean
	python3.7 setup.py sdist bdist_wheel
	twine upload dist/*