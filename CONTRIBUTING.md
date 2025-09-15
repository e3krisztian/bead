# Contributing to Bead

Thank you for your interest in contributing to the Bead project! We welcome contributions from everyone. To ensure a smooth and collaborative process, please review these guidelines.

This document provides instructions for both human developers and AI agents to ensure that all contributions are consistent and high-quality.

## Development Workflow

### Version Control & Commits

We use Git for version control. Please follow these conventions for your commits:

- **Use the [Conventional Commits](https://www.conventionalcommits.org/) format.** This helps us automate releases and makes the project history easy to read.
- Your commit message should clearly explain the **reason** for the change.
- Focus on the **"why"** of the change in the commit body, not just the "what."

### Build, Lint, and Test Commands

Before submitting your contribution, please ensure it passes all checks.

#### Testing
- **Run all tests**: `make test`
- **Run a single test**: `pytest path/to/test_file.py::TestClass::test_method` or `pytest path/to/test_file.py -k "test_name"`
- **Check test coverage**: `pytest --cov=. --cov-report=term-missing`

#### Linting & Formatting
- **Run the linter**: `ruff check .`
- **Format imports automatically**: `isort .`
- **Run the static type checker**: `pytype -k -j auto bead bead_cli __main__.py tests tracelog.py dev/build.py` or `uvx ty check`
- **Run all pre-commit hooks**: `pre-commit run --all-files`

#### Building
- **Build a wheel**: `uv build --wheel`
- **Build executables**: `make executables`

## Code Style Guidelines

### General Principles
- Keep code simple, readable, and self-explanatory.
- Use meaningful names that reveal intent for variables, functions, classes, etc.

### Variables
- If a variable's name conflicts with its content or meaning, it **MUST** be renamed.

### Functions
- Each function should do one thing and do it well.
- Keep functions small (a few lines).
- Prefer a maximum of 2-3 parameters.
- Function names should describe the action being performed.
- If a function's name conflicts with its behavior, it **MUST** be renamed.

### Classes
- Each class should have a single responsibility.
- Follow the [SOLID principles](https://en.wikipedia.org/wiki/SOLID).

### Imports
- Place all imports at the top of the file.
- Use one import per line.
- Our configuration for `isort` enforces single-line, lexicographically sorted imports.

### Formatting
- Maximum line length is 99 characters.
- Use 4 spaces for indentation.
- We follow PEP8 with the following exceptions: W503, W504, E251, E241, E221, E722.

### Types
- Use type hints where they improve clarity and correctness.
- Pyright is configured to ignore unknown parameter/variable types.
- We use `pytype` for static type checking.

### Error Handling
- Handle errors and exceptions properly to ensure robustness.
- Consider the security implications of your code.
- Avoid bare `except:` clauses.

### Comments
- Prefer self-explanatory code over comments.
- Only add comments when they provide information that isn't apparent from the code itself.
- If you add comments, ensure they are kept up-to-date with code changes.

### Database/SQL
- Isolate functions that use direct SQL from other logic.
- These functions should perform a single database operation.
- They should return materialized values (e.g., lists of objects), not cursors or generators.
- Hide the database implementation details from the rest of the application.

### Bead-Specific Knowledge
- `content_id` and `kind` are internal, technical identifiers (long strings). They are not supplied directly by the user.
- Users specify beads and files by their names.
- `bead_ref_base` refers to a user-provided name or filename.
