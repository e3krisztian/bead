.PHONY: test clean executables shiv

test:
	tox

executables:
	dev/build.py

shiv:
	shiv -o executables/bead.shiv -c bead -p '/usr/bin/python -sE' .

container-image:
	podman build --no-cache -t bead-dev - < dev/Containerfile
