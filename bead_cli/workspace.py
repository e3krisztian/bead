from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function

import os
import sys

from bead import tech
from bead.workspace import Workspace, CurrentDirWorkspace
from bead import layouts

from .cmdparse import Command
from .common import die, warning
from .common import DefaultArgSentinel
from .common import OPTIONAL_WORKSPACE, OPTIONAL_ENV
from .common import BEAD_REF_BASE, BEAD_QUERY, get_bead_ref
from . import arg_metavar
from . import arg_help


timestamp = tech.timestamp.timestamp
time_from_timestamp = tech.timestamp.time_from_timestamp


def assert_may_be_valid_name(name):
    valid_syntax = (
        name
        and os.path.sep not in name
        and '/' not in name
        and '\\' not in name
        and ':' not in name
    )
    if not valid_syntax:
        die('Invalid name "{}"'.format(name))


class CmdNew(Command):
    '''
    Create and initialize new workspace directory with a new bead.
    '''

    def declare(self, arg):
        arg('workspace', type=Workspace, metavar=arg_metavar.WORKSPACE,
            help='bead and directory to create')

    def run(self, args):
        workspace = args.workspace
        assert_may_be_valid_name(workspace.bead_name)
        # FIXME: die with message when directory already exists

        kind = tech.identifier.uuid()
        workspace.create(kind)
        print('Created {}'.format(workspace.bead_name))


CURRENT_DIRECTORY = CurrentDirWorkspace()


def WORKSPACE_defaulting_to(default_workspace):
    def opt_workspace(parser):
        parser.arg(
            'workspace', nargs='?', type=Workspace,
            default=default_workspace,
            metavar=arg_metavar.WORKSPACE, help=arg_help.WORKSPACE)
    return opt_workspace


USE_THE_ONLY_BOX = DefaultArgSentinel(
    'if there is exactly one box,' +
    ' store there, otherwise it MUST be specified')


class CmdSave(Command):
    '''
    Save workspace in a box.
    '''

    def declare(self, arg):
        arg('box_name', nargs='?', default=USE_THE_ONLY_BOX, type=str,
            metavar=arg_metavar.BOX, help=arg_help.BOX)
        arg(OPTIONAL_WORKSPACE)
        arg(OPTIONAL_ENV)

    def run(self, args):
        box_name = args.box_name
        workspace = args.workspace
        env = args.get_env()
        assert_valid_workspace(workspace)
        # XXX: (usability) save - support saving directly to a directory outside of workspace
        if box_name is USE_THE_ONLY_BOX:
            boxes = list(env.get_boxes())
            if not boxes:
                warning('No boxes have been defined')
                beadbox = os.path.expanduser('~/BeadBox')
                sys.stderr.write(
                    'Creating and using a new one with name `home` and location {}'
                    .format(beadbox))
                tech.fs.ensure_directory(beadbox)
                env.add_box('home', beadbox)
                env.save()
                # continue with newly created box
                boxes = list(env.get_boxes())
                assert len(boxes) == 1
            if len(boxes) > 1:
                die(
                    'BOX parameter is not optional!\n' +
                    '(more than one boxes exists)')
            box = boxes[0]
        else:
            box = env.get_box(box_name)
            if box is None:
                die('Unknown box: {}'.format(box_name))
        box.store(workspace, timestamp())
        print('Successfully stored bead.')


DERIVE_FROM_BEAD_NAME = DefaultArgSentinel('derive one from bead name')


class CmdDevelop(Command):
    '''
    Unpack a bead as a source tree.

    Bead directory layout is created, but only the source files are
    extracted.
    '''

    def declare(self, arg):
        arg(BEAD_REF_BASE)
        arg(BEAD_QUERY)
        arg(WORKSPACE_defaulting_to(DERIVE_FROM_BEAD_NAME))
        arg('-x', '--extract-output', dest='extract_output',
            default=False, action='store_true',
            help='Extract output data as well (normally it is not needed!).')
        arg(OPTIONAL_ENV)

    def run(self, args):
        extract_output = args.extract_output
        env = args.get_env()
        bead_ref = get_bead_ref(env, args.bead_ref_base, args.bead_query)
        try:
            bead = bead_ref.bead
        except LookupError:
            die('Bead not found!')
        if not bead.is_valid:
            die('Bead is found but damaged')
        if args.workspace is DERIVE_FROM_BEAD_NAME:
            workspace = Workspace(bead.name)
        else:
            workspace = args.workspace

        bead.unpack_to(workspace)
        assert workspace.is_valid

        if extract_output:
            output_directory = workspace.directory / layouts.Workspace.OUTPUT
            bead.unpack_data_to(output_directory)

        print('Extracted source into {}'.format(workspace.directory))
        # XXX: try to load smaller inputs?
        if workspace.inputs:
            print('Input data not loaded, update if needed and load manually')


def assert_valid_workspace(workspace):
    if not workspace.is_valid:
        die('{} is not a valid workspace'.format(workspace.directory))


def print_inputs(env, workspace, verbose):
    assert_valid_workspace(workspace)
    inputs = sorted(workspace.inputs)

    if inputs:
        boxes = env.get_boxes()

        print('Inputs:')
        for input in inputs:
            print('input/' + input.name)
            print('\tName[s]:')
            has_name = False
            for box in boxes:
                timestamp = time_from_timestamp(input.timestamp)
                (
                    exact_match, best_guess, best_guess_timestamp, names
                ) = box.find_names(input.kind, input.content_hash, timestamp)
                #
                has_name = has_name or exact_match or best_guess or names
                if exact_match:
                    print('\t * -r {} {}'.format(box.name, exact_match))
                    names.remove(exact_match)
                elif best_guess:
                    print('\t ? -r {} {}'.format(box.name, best_guess))
                    names.remove(best_guess)
                for name in sorted(names):
                    print('\t [-r {} {}]'.format(box.name, name))
            if verbose or not has_name:
                print('\tBead kind:', input.kind)
                print('\tContent hash:', input.content_hash)
                print('\tFreeze time:', input.timestamp)

        print('')
        unloaded = [
            input.name
            for input in inputs
            if not workspace.is_loaded(input.name)]
        if unloaded:
            print('These inputs are not loaded:')
            unloaded_list = '\t- ' + '\n\t- '.join(unloaded)
            print(unloaded_list.expandtabs(2))
            print('You can "load" or "update" them manually.')
    else:
        print('No inputs defined')


class CmdStatus(Command):
    '''
    Show workspace status - name of bead, inputs and their unpack status.
    '''

    def declare(self, arg):
        arg(OPTIONAL_WORKSPACE)
        arg('-v', '--verbose', default=False, action='store_true',
            help='show more detailed information')
        arg(OPTIONAL_ENV)

    def run(self, args):
        workspace = args.workspace
        verbose = args.verbose
        env = args.get_env()
        # TODO: use a template and render it with passing in all data
        kind_needed = verbose
        if workspace.is_valid:
            print('Bead Name: {}'.format(workspace.bead_name))
            if kind_needed:
                print('Bead kind: {}'.format(workspace.kind))
            print()
            print_inputs(env, workspace, verbose)
        else:
            warning('Invalid workspace ({})'.format(workspace.directory))


class CmdNuke(Command):
    '''
    Delete the workspace, inluding data, code and documentation.
    '''

    def declare(self, arg):
        arg(WORKSPACE_defaulting_to(CURRENT_DIRECTORY))

    def run(self, args):
        workspace = args.workspace
        assert_valid_workspace(workspace)
        directory = workspace.directory
        tech.fs.rmtree(directory)
        print('Deleted workspace {}'.format(directory))
