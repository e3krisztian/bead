import shutil
from typing import TYPE_CHECKING

from bead.infra.fs import Path

from .cmdparse import Command
from .common import die, report_progress

if TYPE_CHECKING:
    from .environment import Environment


def seed_box_index(box_directory: Path, new_index_path: Path):
    """Seed new index with existing box index data to speed up first sync."""
    old_index_path = box_directory / '.index.sqlite'
    if old_index_path.exists():
        try:
            new_index_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(old_index_path, new_index_path)
            print(f'Seeded index from existing data at {old_index_path}')
        except Exception as e:
            print(f'Warning: Failed to seed index: {e}')


class CmdAdd(Command):
    '''
    Define a box.
    '''

    def declare(self, arg):
        arg('name')
        arg('directory', type=Path)

    def run(self, args, env: 'Environment'):
        '''
        Define a box.
        '''
        name: str = args.name
        directory: Path = args.directory

        if not directory.is_dir():
            die(f'"{directory}" is not an existing directory!')
        location = directory.resolve()
        
        # Get the index path before adding the box
        index_path = env.get_box_index_path(name)
        
        # Seed indexing with existing data if available
        seed_box_index(location, index_path)
        
        try:
            env.add_box(name, location)
            env.save()
            print(f'Will remember box {name}')
        except ValueError as e:
            die(f'{e.args[0]}\nCheck the parameters: both name and directory must be unique!')


class CmdList(Command):
    '''
    List boxes.
    '''

    def declare(self, arg):
        pass

    def run(self, args, env: 'Environment'):
        boxes = env.get_all_boxes()

        def print_box(box):
            status = '(enabled)' if box.enabled else '(disabled)'
            print(f'{box.name}: {box.location} {status}')
        if boxes:
            print('Boxes:')
            print('-------------')
            for box in boxes:
                print_box(box)
        else:
            print('There are no defined boxes')


class CmdForget(Command):
    '''
    Remove the named box from the boxes known by the tool.
    '''

    def declare(self, arg):
        arg('name')

    def run(self, args, env: 'Environment'):
        name = args.name

        if env.is_known_box(name):
            env.forget_box(name)
            env.save()
            print(f'Box "{name}" is forgotten')
        else:
            print(f'WARNING: no box defined with "{name}"')


class CmdEnable(Command):
    '''
    Enable a box.
    '''

    def declare(self, arg):
        arg('name')

    def run(self, args, env: 'Environment'):
        name = args.name

        try:
            env.enable_box(name)
            env.save()
            print(f'Box "{name}" is enabled')
        except ValueError as e:
            print('ERROR:', *e.args)


class CmdDisable(Command):
    '''
    Disable a box.
    '''

    def declare(self, arg):
        arg('name')

    def run(self, args, env: 'Environment'):
        name = args.name

        try:
            env.disable_box(name)
            env.save()
            print(f'Box "{name}" is disabled')
        except ValueError as e:
            print('ERROR:', *e.args)


def reindex(box_meta):
    '''Rebuild index for a single box by syncing with filesystem.'''
    try:
        print(f'Rebuilding index for box "{box_meta.name}" at {box_meta.directory}')
        # Remove any corrupted index and create fresh box
        box_meta.remove_index()
        box = box_meta.create_box()
        return report_progress('Rebuilding', box.index.sync())
    except Exception as e:
        print(f'  ✗ Failed: {e}')
        return False


def reindex_all(enabled_boxes):
    '''Rebuild indexes for all enabled boxes using flat metadata.'''
    if not enabled_boxes:
        print('No enabled boxes defined')
        return

    print(f'Rebuilding indexes for {len(enabled_boxes)} box(es)...')
    success_count = 0

    for box_meta in enabled_boxes:
        if reindex(box_meta):
            success_count += 1

    print(f'Completed: {success_count}/{len(enabled_boxes)} boxes rebuilt successfully')


class CmdReindex(Command):
    '''
    Rebuild the SQLite index for a specific box or all boxes.

    If no arguments are provided and only one box is defined, that box will be rebuilt automatically.
    '''

    def declare(self, arg):
        arg('box_name', nargs='?', help='Box name to rebuild (optional if only one box exists)')
        arg('--all', action='store_true', help='Rebuild all boxes')

    def run(self, args, env: 'Environment'):
        if args.all:
            reindex_all(env.get_meta_boxes())
            return

        if args.box_name:
            # Specific box requested - use meta boxes to avoid circular dependency
            box_name = args.box_name
            box_meta = next((b for b in env.get_meta_boxes() if b.name == box_name), None)
            if not box_meta:
                print(f'ERROR: Unknown box "{box_name}"')
                return

            # Reindex the specified box
            reindex(box_meta)
            return

        # No arguments provided - check if we can auto-detect single box
        # Use meta boxes to avoid circular dependency with corrupted indexes
        enabled_boxes = env.get_meta_boxes()

        if len(enabled_boxes) == 1:
            # Auto-use the single enabled box
            reindex(enabled_boxes[0])
            return
        elif len(enabled_boxes) == 0:
            print('ERROR: No boxes defined. Use "bead box add" to define a box first.')
            return
        else:
            print('ERROR: Multiple boxes defined. Must specify box name or --all')
            return


def index(box):
    '''Create or update index for a single box.'''
    try:
        print(f'Indexing box "{box.name}" at {box.location}')
        return report_progress('Indexing', box.index.sync())
    except Exception as e:
        print(f'  ✗ Failed: {e}')
        return False


def index_all(boxes):
    '''Create or update indexes for all boxes.'''
    if not boxes:
        print('No boxes defined')
        return

    print(f'Indexing {len(boxes)} box(es)...')
    success_count = 0

    for box in boxes:
        if index(box):
            success_count += 1

    print(f'Completed: {success_count}/{len(boxes)} boxes indexed successfully')


class CmdIndex(Command):
    '''
    Create or update the SQLite index for a specific box or all boxes.

    If no arguments are provided and only one box is defined, that box will be indexed automatically.
    '''

    def declare(self, arg):
        arg('box_name', nargs='?', help='Box name to index (optional if only one box exists)')
        arg('--all', action='store_true', help='Index all boxes')

    def run(self, args, env: 'Environment'):
        if args.all:
            index_all(env.get_boxes())
            return

        if args.box_name:
            # Specific box requested - use normal pattern like other CLI commands
            box_name = args.box_name
            try:
                box = env.get_box(box_name)
            except LookupError:
                print(f'ERROR: Unknown box "{box_name}"')
                return

            index(box)
            return

        # No arguments provided - check if we can auto-detect single box
        boxes = env.get_boxes()
        if len(boxes) == 1:
            # Auto-use the single box
            index(boxes[0])
            return
        elif len(boxes) == 0:
            print('ERROR: No boxes defined. Use "bead box add" to define a box first.')
            return
        else:
            print('ERROR: Multiple boxes defined. Must specify box name or --all')
            return
