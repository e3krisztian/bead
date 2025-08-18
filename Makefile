.PHONY: test clean executables shiv

test:
	tox

executables: git-info
	git add bead_cli/git_info.py
	dev/build.py
	git rm -f bead_cli/git_info.py

shiv: git-info
	shiv -o executables/bead.shiv -c bead -p '/usr/bin/python -sE' .

git-info:
	./add-git-info.sh

container-image:
	podman build -t bead-dev - < dev/Containerfile
