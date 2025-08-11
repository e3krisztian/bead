'''
A minimalist (by intention) wrapper/dispatcher for argparse when you need
to support more than one commands or even a hierarchy of them (svn/git).

You will need to know `argparse.ArgumentParser.add_argument` as it is used
to declare arguments, but use named parameters except for the option names.

For single command scripts, be more minimalist and just use argparse directly.

:)
'''


import argparse
from collections.abc import Sequence
import shlex
from typing import Any

import argcomplete


class Command:
    '''
    Base class for application defined command classes that link
    argparse (user input), and a function.
    '''

    FORMATTER_CLASS = argparse.HelpFormatter

    def declare(self, arg):
        '''
        Declare command arguments by overriding it.

        `arg` is `Parser.arg` - think of it as `argparser.add_argument`
        e.g. these all work:
        arg('param')
        arg('--option', help='changes how the command behaves')

        There is also an extension, see `Parser.arg` for details.
        '''
        pass

    @property
    def description(self):
        '''
        Command description.

        Defaults to the class docstring.
        '''
        assert self.__doc__ is not None, self.__class__
        return self.__doc__

    def run(self, args):
        '''
        This is the function that gets called with the parsed arguments.

        You will want to override it!
        '''
        raise NotImplementedError


class Parser:
    '''
    Wrapper for `argparse.ArgumentParser` with conveniences for multi-command
    parsers.
    '''

    argparser: argparse.ArgumentParser

    def __init__(self, argparser: argparse.ArgumentParser, defaults: dict) -> None:
        '''
        Wrap an `argparse.ArgumentParser`.

        See `new` on how to make a Parser.
        '''
        self.argparser = argparser
        self.defaults = defaults

        # This is ugly :(
        # subparsers should be an `argparse` implementation detail, but is not
        self.__subparsers = None

    @classmethod
    def new(cls, defaults: dict, *args: Any, **kwargs: Any) -> 'Parser':
        '''
        Create a new `Parser`.

        Arguments are passed to `argparse.ArgumentParser()` and the argparser
        is wrapped as `Parser`.

        This eliminates the need for users to import argparse.
        '''
        return cls(argparse.ArgumentParser(*args, **kwargs), defaults)

    @property
    def _subparsers(self):
        if self.__subparsers is None:
            self.__subparsers = self.argparser.add_subparsers()
        return self.__subparsers

    def _make_command(self, commandish: Command | type[Command]) -> Command:
        '''
        Make a proper Command instance.

        This is a convenience function to allow for easier to read client code,
        while still remaining quite strict on what is supported.
        '''
        if isinstance(commandish, Command):
            return commandish

        if issubclass(commandish, Command):
            instance = commandish()
            return instance

        raise TypeError

    def arg(self, *args: Any, **kwargs: Any) -> None:
        '''
        Declare one or more arguments.

        Same as `argparse.ArgumentParser.add_argument` with an extension:
        when the first and only parameter is a function, it is called with
        the parser to do some non-trivial work, like adding an argument group.

        The argument help is fixed up to show the default value.
        '''
        assert args
        if not kwargs and len(args) == 1 and callable(args[0]):
            args[0](self)
        else:
            arg_kwargs = dict(kwargs)
            if 'default' in kwargs:
                # extend help with default
                arg_kwargs['help'] = (
                    f"{kwargs.get('help', '')} (default: {kwargs['default']!s})")
            self.argparser.add_argument(*args, **arg_kwargs)

    def command(self, name: str, commandish: Command | type[Command], title: str) -> None:
        '''
        Declare a command.

        Its name will be `name` and its arguments are defined by `commandish`
        Its help line will be `title`, while its help will be generated from
        its arguments.
        '''
        command = self._make_command(commandish)
        parser = self._subparsers.add_parser(
            name,
            help=title,
            description=command.description,
            formatter_class=command.FORMATTER_CLASS
        )
        command.declare(self.__class__(parser, self.defaults).arg)
        parser.set_defaults(_cmdparse__run=command.run)

    def commands(self, *commands_sequence: tuple[str, Command | type[Command], str]) -> None:
        '''
        Declare any number of commands in one step.

        Takes any number of (name, command, title) tuples.
        '''
        for name, command, title in commands_sequence:
            self.command(name, command, title)

    def group(self, name: str, title: str = '', help: str | None = None) -> 'Parser':
        '''
        Declare a command group.

        Returns a `Parser` for the group to declare subcommands.
        '''
        parser = self._subparsers.add_parser(
            name, help=title + '...', description=help)
        return self.__class__(parser, self.defaults)

    def dispatch(self, argv: Sequence[str]) -> int:
        '''
        Parse `argv` and dispatch to the appropriate command.
        '''
        def print_help(args):
            print(
                'ERROR: not a full command <%s>\n'
                % ' '.join(shlex.quote(arg) for arg in argv))
            self.argparser.print_help()
            return -1

        try:
            args = self.argparser.parse_args(argv)
        except SystemExit:
            # argparse throws SystemExit when help triggered,
            #   which is a surprising API:
            # https://bugs.python.org/issue10506
            #
            # also most of the test runners misbehaves/misbehaved on it:
            # https://github.com/testing-cabal/testtools/issues/144
            #
            # this is worked around here
            return -1
        run = getattr(args, '_cmdparse__run', print_help)
        return run(args) or 0

    def autocomplete(self):
        """Enable shell autocomplete"""
        argcomplete.autocomplete(self.argparser)
