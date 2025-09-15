.PHONY: test clean executables

test:
	uv run python -m pytest --cov=. --cov-report=term-missing
	uv run ruff check .

executables:
	dev/build.py

vm:
	podman build --no-cache -t bead-dev - < dev/Containerfile
