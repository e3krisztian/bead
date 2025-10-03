"""Tests for debug logging functionality."""

import logging

import pytest

from bead_cli.main import setup_logging


def reset_logging():
    """Properly reset logging handlers."""
    # Close and remove all handlers
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)
    # Reset logger levels
    logging.root.setLevel(logging.WARNING)
    for logger_name in list(logging.Logger.manager.loggerDict.keys()):
        logger = logging.getLogger(logger_name)
        logger.handlers = []
        logger.setLevel(logging.NOTSET)


@pytest.fixture(autouse=True)
def reset_logging_fixture():
    """Reset logging before and after each test."""
    reset_logging()
    yield
    # Flush all handlers before closing them
    for handler in logging.root.handlers:
        handler.flush()
    reset_logging()


def test_debug_logging_creates_file(tmp_path):
    """Test that debug mode creates a log file."""
    log_dir = tmp_path / 'logs'

    setup_logging(debug=True, log_dir=log_dir)

    log_file = log_dir / 'bead-debug.log'
    assert log_file.exists()

    # Test that logging works
    logger = logging.getLogger('bead')
    logger.debug('test debug message')

    # Check that the message was written
    log_content = log_file.read_text()
    assert 'test debug message' in log_content
    assert 'bead' in log_content


def test_non_debug_no_file(tmp_path):
    """Test that non-debug mode doesn't create log files."""
    log_dir = tmp_path / 'logs'

    setup_logging(debug=False, log_dir=log_dir)

    log_file = log_dir / 'bead-debug.log'
    assert not log_file.exists()


def test_debug_logging_level(tmp_path):
    """Test that debug mode sets appropriate log level."""
    log_dir = tmp_path / 'logs'

    setup_logging(debug=True, log_dir=log_dir)

    logger = logging.getLogger('bead')
    logger.debug('debug message')
    logger.info('info message')

    log_content = (log_dir / 'bead-debug.log').read_text()
    assert 'debug message' in log_content
    assert 'info message' in log_content


def test_non_debug_logging_level(tmp_path):
    """Test that non-debug mode only logs warnings and above."""
    log_dir = tmp_path / 'logs'

    setup_logging(debug=False, log_dir=log_dir)

    # Check that the root logger is set to WARNING level
    assert logging.root.level == logging.WARNING
