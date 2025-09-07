import contextlib
import io
import os
import tempfile

from bead import tech
import bead.zipopener
from tracelog import TRACELOG

from .environment import Environment
from .main import run


@contextlib.contextmanager
def chdir(directory):
    cwd = os.getcwd()
    try:
        os.chdir(directory)
        yield
    finally:
        os.chdir(cwd)


@contextlib.contextmanager
def setenv(variable: str, value: str):
    old_value = os.environ.get(variable)
    try:
        os.environ[variable] = value
        yield
    finally:
        if old_value is None:
            del os.environ[variable]
        else:
            os.environ[variable] = old_value


class Fixture:
    def __init__(self):
        self.__cleanups = []

    def setUp(self):
        pass

    def cleanUp(self):
        while self.__cleanups:
            cleanup, args, kwargs = self.__cleanups[-1]
            cleanup(*args, **kwargs)
            del self.__cleanups[-1]

    def addCleanup(self, cleanup, *args, **kwargs):
        self.__cleanups.append((cleanup, args, kwargs))

    def useFixture(self, fixture):
        fixture.setUp()
        self.addCleanup(fixture.cleanUp)
        return fixture

    def __enter__(self):
        self.setUp()
        return self

    def __exit__(self, *_exc):
        self.cleanUp()


@contextlib.contextmanager
def environment(robot):
    '''
    Context manager - enable running code in the context of the robot.
    '''
    with setenv('HOME', robot.home.as_posix()):
        with chdir(robot.cwd):
            try:
                yield Environment.from_dir(robot.config_dir)
            except BaseException as e:
                robot.retval = e
                raise


class TempDir(Fixture):
    path: tech.fs.Path

    def setUp(self):
        super().setUp()
        self.path = tech.fs.Path(tempfile.mkdtemp())
        # we need our own rmtree, that can remove read only files as well
        self.addCleanup(tech.fs.rmtree, self.path, ignore_errors=True)


class _CaptureStream(Fixture):

    def __init__(self, redirector):
        self.redirector = redirector
        self.string_stream = io.StringIO()
        super().__init__()

    def setUp(self):
        super().setUp()
        redirect = self.redirector(self.string_stream)
        redirect.__enter__()
        self.addCleanup(lambda: redirect.__exit__(None, None, None))

    @property
    def text(self):
        return self.string_stream.getvalue()


def CaptureStdout():
    return _CaptureStream(contextlib.redirect_stdout)


def CaptureStderr():
    return _CaptureStream(contextlib.redirect_stderr)


class Robot(Fixture):
    '''
    Represents a fake user.

    All operations are isolated from the test runner user's environment.
    They work in a dedicated environment with temporary home, config
    and working directories.
    '''

    cwd: tech.fs.Path
    base_dir: tech.fs.Path

    def setUp(self):
        super().setUp()
        self.base_dir = self.useFixture(TempDir()).path
        TRACELOG('makedirs', self.home)
        os.makedirs(self.home)
        self.cd(self.home)

    def cleanUp(self):
        super().cleanUp()

    @property
    def config_dir(self):
        return self.base_dir / 'config'

    @property
    def home(self):
        return self.base_dir / 'home'

    def _path(self, path):
        '''
        Convert relative paths to absolute paths
        '''
        if os.path.isabs(path):
            return path
        else:
            return tech.fs.Path(os.path.normpath(self.cwd / path))

    def cd(self, dir):
        '''
        Change to directory
        '''
        self.cwd = self._path(dir)
        TRACELOG(dir, realdir=self.cwd)
        assert os.path.isdir(self.cwd)

    @property
    def environment(self):
        '''
        Context manager - enable running code in the context of this robot.
        '''
        return environment(self)

    def cli(self, *args: str | tech.fs.Path):
        '''
        Imitate calling the command line tool with the given args
        '''
        TRACELOG(*args)
        if len(args) == 1:
            # special case: input is a single string
            arg = args[0]
            assert isinstance(arg, str)
            str_args = arg.split()
            if len(str_args) > 1:
                return self.cli(*str_args)
        else:
            str_args = [(arg if isinstance(arg, str) else arg.as_posix()) for arg in args]

        with self.environment:
            with CaptureStdout() as stdout, CaptureStderr() as stderr:
                try:
                    self.retval = run(''.__class__(self.config_dir), str_args)
                    assert self.retval == 0
                except BaseException as e:
                    TRACELOG(EXCEPTION=e)
                    raise
                finally:
                    # invalidate zip file cache
                    # it would prevent deleting opened files on windows
                    # and would keep zip files open, even after they are removed on unix
                    bead.zipopener.close_all()

                    self.stdout = stdout.text
                    self.stderr = stderr.text
                    #
                    if self.stdout:
                        TRACELOG(STDOUT=self.stdout)
                    if self.stderr:
                        TRACELOG(STDERR=self.stderr)

    def ls(self, directory=None):
        directory = self._path(directory or self.cwd)
        return [
            directory / filename
            for filename in os.listdir(directory)]

    def write_file(self, path, content):
        assert not os.path.isabs(path)
        TRACELOG(path, content, realpath=self.cwd / path)
        tech.fs.write_file(self.cwd / path, content)

    def read_file(self, filename):
        return tech.fs.read_file(self.cwd / filename)

    def reset(self):
        '''
        Forget all boxes by removing the user's config.

        All other files, workspaces remain available.
        '''
        TRACELOG('rmtree', self.config_dir)
        tech.fs.rmtree(self.config_dir)
