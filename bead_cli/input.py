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
from .common import (
    BEAD_REF_BASE_defaulting_to, BEAD_QUERY, get_bead_ref, get_query_index, BoxQueryReference)
from bead import spec as bead_spec
from bead.tech.timestamp import time_from_timestamp


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
NEWEST_VERSION = DefaultArgSentinel('same bead, newest version')
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
        arg(BEAD_QUERY)  # XXX: add: arg(BEAD_TIME) as a generic query should not be supported
        arg(OPTIONAL_WORKSPACE)
        arg(OPTIONAL_ENV)

    def run(self, args):
        input_nick = args.input_nick
        bead_ref_base = args.bead_ref_base
        workspace = args.workspace
        env = args.get_env()

        if bead_ref_base is USE_INPUT_NICK:
            bead_ref_base = input_nick

        bead_ref = get_bead_ref(env, bead_ref_base, args.bead_query)

        try:
            bead = bead_ref.bead
        except LookupError:
            die('Not a known bead name: {}'.format(bead_ref_base))

        _check_load_with_feedback(
            workspace, args.input_nick, bead, bead_ref_base)


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
        arg(BEAD_REF_BASE_defaulting_to(NEWEST_VERSION))
        arg(BEAD_QUERY)
        arg(OPTIONAL_WORKSPACE)
        arg(OPTIONAL_ENV)

    def run(self, args):
        input_nick = args.input_nick
        bead_ref_base = args.bead_ref_base
        workspace = args.workspace
        env = args.get_env()
        if input_nick is ALL_INPUTS:
            # TODO: update: assert there is no other argument
            for input in workspace.inputs:
                try:
                    _update(env, workspace, input)
                except LookupError:
                    if workspace.is_loaded(input.name):
                        print('{} is already newest ({})'.format(input.name, input.timestamp))
                    else:
                        warning('Can not find bead for {}'.format(input.name))
            print('All inputs are up to date.')
        else:
            input = workspace.get_input(input_nick)
            # FIXME: update: is an inconsistent mess
            # should be simplified to this 3 cases:
            #  - update to last version (index==-1)
            #  - update to version closest to given time (not yet supported)
            #  - update to next/previous version
            # it should work without loading the bead if it was unloaded
            if bead_ref_base is NEWEST_VERSION:
                bead_ref = BoxQueryReference(env.get_boxes())
                query = [(bead_spec.KIND, input.kind)]
                bead_ref.add_query(query)
                # assert len(args.bead_query) == 0
                time = time_from_timestamp(input.timestamp)
                index = get_query_index(args)
                if index < -1:
                    bead_ref.add_query([(bead_spec.OLDER_THAN, time)])
                    index += 1
                elif index > -1:
                    bead_ref.add_query([(bead_spec.NEWER_THAN, time)])
                bead_ref.set_index(index)
            else:
                bead_ref = get_bead_ref(env, bead_ref_base, args.bead_query)
                assert -1 == get_query_index(args)
            try:
                _update(env, workspace, input, bead_ref)
            except LookupError:
                die('Can not find matching bead')


def _update(env, workspace, input, bead_ref=NEWEST_VERSION):
    if bead_ref is NEWEST_VERSION:
        query = [
            (bead_spec.KIND, input.kind),
            (bead_spec.NEWER_THAN, time_from_timestamp(input.timestamp))]
        bead_ref = BoxQueryReference(env.get_boxes())
        bead_ref.add_query(query)

    replacement = bead_ref.bead
    _check_load_with_feedback(workspace, input.name, replacement)


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


def _check_load_with_feedback(
        workspace, input_name, bead, bead_name=None):
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
