
import pytest

from bead_cli.main import main


class UnhandledError(Exception):
    pass


def _deep_unhandled_exception(n=4):
    if n == 0:
        raise UnhandledError
    _deep_unhandled_exception(n - 1)


def run_raise_unhandled(config_dir, state_dir, argv):
    _deep_unhandled_exception()


def test_unhandled_error(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # Set BEAD_LOG_DIR to tmp_path so we can find the error file
    log_dir = tmp_path / 'logs'
    monkeypatch.setenv('BEAD_LOG_DIR', str(log_dir))

    argv = ['1', 'unhandled', 'ecxeptipons']
    argv_text = f'{argv}'
    monkeypatch.setattr('sys.argv', argv)

    with pytest.raises(SystemExit):
        main(run=run_raise_unhandled)

    stderr = capsys.readouterr().err

    # Error file should be in log_dir/errors/
    error_dir = log_dir / 'errors'
    error_files = list(error_dir.glob('error_*.txt'))
    assert len(error_files) == 1
    error_report_path = error_files[0]

    # stderr is what the user see
    assert 'UnhandledError' in stderr
    assert f'{error_report_path}' in stderr

    # error report is the file written with the error details
    error_report_text = error_report_path.read_text()
    assert 'UnhandledError' in error_report_text
    assert argv_text in error_report_text
    assert f'cwd = {tmp_path}' in error_report_text
    assert error_report_text.count('_deep_unhandled_exception') > 3


def test_keyboard_interrupt(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # Set BEAD_LOG_DIR to tmp_path
    log_dir = tmp_path / 'logs'
    monkeypatch.setenv('BEAD_LOG_DIR', str(log_dir))

    def run_raise_keyboardinterrupt(*args, **kwargs):
        raise KeyboardInterrupt

    with pytest.raises(SystemExit):
        main(run=run_raise_keyboardinterrupt)

    stderr = capsys.readouterr().err

    # No error file should be created for keyboard interrupt
    error_dir = log_dir / 'errors'
    if error_dir.exists():
        assert [] == list(error_dir.glob('error_*.txt'))

    # stderr is what the user see
    assert 'Interrupted' in stderr


def test_help(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # Set BEAD_LOG_DIR to tmp_path
    log_dir = tmp_path / 'logs'
    monkeypatch.setenv('BEAD_LOG_DIR', str(log_dir))

    argv = ['.', '--help']
    monkeypatch.setattr('sys.argv', argv)

    with pytest.raises(SystemExit):
        main()

    stdout = capsys.readouterr().out

    # No error file should be created for help
    error_dir = log_dir / 'errors'
    if error_dir.exists():
        assert [] == list(error_dir.glob('error_*.txt'))

    # stdout is what the user see
    assert 'usage:' in stdout.lower()
    assert '-h, --help' in stdout
