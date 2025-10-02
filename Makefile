.PHONY: test clean executables

test:
	uv run python -m pytest --cov=. --cov-report=term-missing
	uv run ruff check .

executables:
	dev/build.py

vm:
	podman build --no-cache -t bead-dev - < dev/Containerfile

clean:
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "__pycache__" -exec rmdir {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -f .coverage
