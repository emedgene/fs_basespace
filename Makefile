clean:
	rm -rf dist
	rm -rf build

build-pip-src-package:
	python setup.py sdist

build-pip-package:
	cd ./basespace_protected;	python setup.py sdist