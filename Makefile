wheel:
	python3 -m build -w

clean:
	rm -f MANIFEST
	rm -rf dist build
	rm -rf **/*.pyc
	rm -rf **/*.pyo
	rm -rf **/*.pyo
	rm -rf *.egg-info/
