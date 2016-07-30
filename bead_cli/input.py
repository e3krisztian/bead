from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function

from .cmdparse import Command

from . import arg_metavar
from . import arg_help
from .common import (
    OPTIONAL_WORKSPACE, OPTIONAL_ENV,
    DefaultArgSentinel,
    CurrentDirWorkspace,
    die, warning
)
from .common import BEAD_REF_BASE_defaulting_to, BEAD_OFFSET, BEAD_TIME, resolve_bead, TIME_LATEST
from bead.box import UnionBox
import bead.spec as bead_spec

# input_nick
ALL_INPUTS = DefaultArgSentinel('all inputs')


def OPTIONAL_INPUT_NICK(parser):
    '''
    Declare `input_nick` as optional parameter
    '''
    parser.arg(
        'input_nick', type=type(''), nargs='?', default=ALL_INPUTS,
        metavar=arg_metavar.INPUT_NICK, help=arg_help.INPUT_NICK)


def INPUT_NICK(parser):
    '''
    Declare `input_nick` as mandatory parameter
    '''
    parser.arg(
        'input_nick',
        metavar=arg_metavar.INPUT_NICK, help=arg_help.INPUT_NICK)


# bead_ref
SAME_KIND_NEWEST_VERSION = DefaultArgSentinel('same bead, newest version')
USE_INPUT_NICK = DefaultArgSentinel('use {}'.format(arg_metavar.INPUT_NICK))
# default workspace
CURRENT_DIRECTORY = CurrentDirWorkspace()


class CmdAdd(Command):
    '''
    Make data from another bead available in the input directory.
    '''

    def declare(self, arg):
        arg(INPUT_NICK)
        arg(BEAD_REF_BASE_defaulting_to(USE_INPUT_NICK))
        arg(BEAD_TIME)
        arg(OPTIONAL_WORKSPACE)
        arg(OPTIONAL_ENV)

    def run(self, args):
        input_nick = args.input_nick
        bead_ref_base = args.bead_ref_base
        workspace = args.workspace
        env = args.get_env()

        if bead_ref_base is USE_INPUT_NICK:
            bead_ref_base = input_nick

        try:
            bead = resolve_bead(env, bead_ref_base, args.bead_time)
        except LookupError:
            die('Not a known bead name: {}'.format(bead_ref_base))

        _check_load_with_feedback(workspace, args.input_nick, bead, bead_ref_base)


class CmdDelete(Command):
    '''
    Forget all about an input.
    '''

    def declare(self, arg):
        arg(INPUT_NICK)
        arg(OPTIONAL_WORKSPACE)

    def run(self, args):
        input_nick = args.input_nick
        workspace = args.workspace
        workspace.delete_input(input_nick)
        print('Input {} is deleted.'.format(input_nick))


class CmdUpdate(Command):
    '''
    Update input[s] to newest version or defined bead.
    '''

    def declare(self, arg):
        arg(OPTIONAL_INPUT_NICK)
        arg(BEAD_REF_BASE_defaulting_to(SAME_KIND_NEWEST_VERSION))
        arg(BEAD_TIME)
        arg(BEAD_OFFSET)
        arg(OPTIONAL_WORKSPACE)
        arg(OPTIONAL_ENV)

    def run(self, args):
        input_nick = args.input_nick
        bead_ref_base = args.bead_ref_base
        workspace = args.workspace
        env = args.get_env()
        if input_nick is ALL_INPUTS:
            # TODO: update: assert there is no other argument
            unionbox = UnionBox(env.get_boxes())
            for input in workspace.inputs:
                try:
                    bead = unionbox.get_at(bead_spec.KIND, input.kind, args.bead_time)
                except LookupError:
                    if workspace.is_loaded(input.name):
                        print('{} is already newest ({})'.format(input.name, input.timestamp))
                    else:
                        warning('Can not find bead for {}'.format(input.name))
                else:
                    _check_load_with_feedback(workspace, input.name, bead)

            print('All inputs are up to date.')
        else:
            input = workspace.get_input(input_nick)
            if bead_ref_base is SAME_KIND_NEWEST_VERSION:
                # handle --prev --next --time
                unionbox = UnionBox(env.get_boxes())
                if args.bead_offset:
                    assert args.bead_time == TIME_LATEST
                    context = unionbox.get_context(bead_spec.KIND, input.kind, input.timestamp)
                    if args.bead_offset == 1:
                        bead = context.next
                    else:
                        bead = context.prev
                else:
                    bead = unionbox.get_at(bead_spec.KIND, input.kind, args.bead_time)
            else:
                # normal path - same as input add, develop
                assert args.bead_offset == 0
                bead = resolve_bead(env, bead_ref_base, args.bead_time)
            if bead:
                _check_load_with_feedback(workspace, input.name, bead, bead_ref_base)
            else:
                die('Can not find matching bead')


class CmdLoad(Command):
    '''
    Put defined input data in place.
    '''

    def declare(self, arg):
        arg(OPTIONAL_INPUT_NICK)
        arg(OPTIONAL_WORKSPACE)
        arg(OPTIONAL_ENV)

    def run(self, args):
        input_nick = args.input_nick
        workspace = args.workspace
        env = args.get_env()
        if input_nick is ALL_INPUTS:
            inputs = workspace.inputs
            if inputs:
                for input in inputs:
                    _load(env, workspace, input)
            else:
                warning('No inputs defined to load.')
        else:
            _load(env, workspace, workspace.get_input(input_nick))


def _load(env, workspace, input):
    assert input is not None
    if not workspace.is_loaded(input.name):
        try:
            bead = env.get_bead(input.kind, input.content_hash)
        except LookupError:
            warning(
                'Could not find archive for {} - not loaded!'
                .format(input.name))
        else:
            _check_load_with_feedback(workspace, input.name, bead)
    else:
        print('Skipping {} (already loaded)'.format(input.name))


def _check_load_with_feedback(workspace, input_name, bead, bead_name=None):
    if bead.is_valid:
        if workspace.is_loaded(input_name):
            workspace.unload(input_name)
        workspace.load(input_name, bead)
        if bead_name:
            print('{} loaded on {}.'.format(bead_name, input_name))
        else:
            print('Loaded {}.'.format(input_name))
    else:
        warning(
            'Bead for {} is found but damaged - not loading.'
            .format(input_name))
