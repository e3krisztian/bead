from typing import TYPE_CHECKING, Generator
from tqdm import tqdm

from bead import tech
from bead.box_index import IndexingError, IndexingProgress

from .cmdparse import Command

if TYPE_CHECKING:
    from .environment import Environment


class CmdAdd(Command):
    '''
    Define a box.
    '''

    def declare(self, arg):
        arg('name')
        arg('directory', type=tech.fs.Path)

    def run(self, args, env: 'Environment'):
        '''
        Define a box.
        '''
        name: str = args.name
        directory: tech.fs.Path = args.directory

        if not directory.is_dir():
            print(f'ERROR: "{directory}" is not an existing directory!')
            return
        location = directory.resolve()
        try:
            env.add_box(name, location)
            env.save()
            print(f'Will remember box {name}')
        except ValueError as e:
            print('ERROR:', *e.args)
            print('Check the parameters: both name and directory must be unique!')


class CmdList(Command):
    '''
    List boxes.
    '''

    def declare(self, arg):
        pass

    def run(self, args, env: 'Environment'):
        boxes = env.get_boxes()

        def print_box(box):
            print(f'{box.name}: {box.location}')
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


def _report_progress(
    description: str,
    progress_generator: Generator[IndexingProgress, None, None]
) -> bool:
    '''
    Consume a progress generator, report status using tqdm, and return success.
    '''
    errors: list[IndexingError] = []
    # Initialize with a total of 0; it will be updated on the first iteration.
    with tqdm(total=0, desc=f'  {description}', unit=' files', leave=False) as pbar:
        try:
            for progress in progress_generator:
                if pbar.total != progress.total:
                    pbar.total = progress.total
                    # Refresh to show the total immediately
                    pbar.refresh()

                pbar.update(1)
                if progress.latest_error:
                    errors.append(progress.latest_error)
                    # tqdm.write is the safe way to print messages without breaking the bar
                    tqdm.write(
                        f"  ✗ Error indexing {progress.path.name}: {progress.latest_error.reason}"
                    )
        except Exception as e:
            # Catch fatal errors from the generator itself (e.g., DB connection)
            tqdm.write(f"  ✗ A fatal error occurred: {e}")
            # Ensure the progress bar is cleared on fatal error
            pbar.close()
            return False

    if errors:
        print(f'  ✗ Completed with {len(errors)} error(s).')
        return False
    else:
        print('  ✓ Done')
        return True



def reindex(box):
    '''Rebuild index for a single box.'''
    from bead.box_index import BoxIndex
    try:
        print(f'Rebuilding index for box "{box.name}" at {box.location}')
        box_index = BoxIndex(box.location)
        return _report_progress('Rebuilding', box_index.rebuild())
    except Exception as e:
        print(f'  ✗ Failed: {e}')
        return False


def reindex_directory(directory):
    '''Rebuild index for a directory.'''
    from bead.box_index import BoxIndex
    try:
        print(f'Rebuilding index for directory {directory}')
        box_index = BoxIndex(directory)
        return _report_progress('Rebuilding', box_index.rebuild())
    except Exception as e:
        print(f'  ✗ Failed: {e}')
        return False


def reindex_all(boxes):
    '''Rebuild indexes for all boxes.'''
    if not boxes:
        print('No boxes defined')
        return

    print(f'Rebuilding indexes for {len(boxes)} box(es)...')
    success_count = 0

    for box in boxes:
        if reindex(box):
            success_count += 1

    print(f'Completed: {success_count}/{len(boxes)} boxes rebuilt successfully')


class CmdReindex(Command):
    '''
    Rebuild the SQLite index for a specific box, directory, or all boxes.

    If no arguments are provided and only one box is defined, that box will be rebuilt automatically.
    '''

    def declare(self, arg):
        def setup_mutually_exclusive_args(parser):
            group = parser.argparser.add_mutually_exclusive_group()
            group.add_argument('--box', help='Box name to rebuild')
            group.add_argument('--dir', type=tech.fs.Path, help='Box directory to rebuild')
            group.add_argument('--all', action='store_true', help='Rebuild all boxes')

        arg(setup_mutually_exclusive_args)

    def run(self, args, env: 'Environment'):
        if not any([args.box, args.dir, args.all]):
            # No arguments provided - check if we can auto-detect single box
            boxes = env.get_boxes()
            if len(boxes) == 1:
                # Auto-use the single box
                reindex(boxes[0])
                return
            elif len(boxes) == 0:
                print('ERROR: No boxes defined. Use "bead box add" to define a box first.')
                return
            else:
                print('ERROR: Multiple boxes defined. Must specify either --box, --dir, or --all')
                return

        if args.all:
            reindex_all(env.get_boxes())
        elif args.dir:
            # Rebuild specific directory
            directory = args.dir
            if not directory.is_dir():
                print(f'ERROR: "{directory}" is not an existing directory!')
                return
            reindex_directory(directory)
        else:
            # Rebuild specific box by name
            box_name = args.box
            if not env.is_known_box(box_name):
                print(f'ERROR: Unknown box "{box_name}"')
                return

            box = env.get_box(box_name)
            if box is None:
                print(f'ERROR: Box "{box_name}" not found')
                return

            reindex(box)


def index(box):
    '''Create or update index for a single box.'''
    from bead.box_index import BoxIndex
    try:
        print(f'Indexing box "{box.name}" at {box.location}')
        box_index = BoxIndex(box.location)
        return _report_progress('Indexing', box_index.sync())
    except Exception as e:
        print(f'  ✗ Failed: {e}')
        return False


def index_directory(directory):
    '''Create or update index for a directory.'''
    from bead.box_index import BoxIndex
    try:
        print(f'Indexing directory {directory}')
        box_index = BoxIndex(directory)
        return _report_progress('Indexing', box_index.sync())
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
    Create or update the SQLite index for a specific box, directory, or all boxes.

    If no arguments are provided and only one box is defined, that box will be indexed automatically.
    '''

    def declare(self, arg):
        def setup_mutually_exclusive_args(parser):
            group = parser.argparser.add_mutually_exclusive_group()
            group.add_argument('--box', help='Box name to index')
            group.add_argument('--dir', type=tech.fs.Path, help='Box directory to index')
            group.add_argument('--all', action='store_true', help='Index all boxes')

        arg(setup_mutually_exclusive_args)

    def run(self, args, env: 'Environment'):
        if not any([args.box, args.dir, args.all]):
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
                print('ERROR: Multiple boxes defined. Must specify either --box, --dir, or --all')
                return

        if args.all:
            index_all(env.get_boxes())
        elif args.dir:
            # Index specific directory
            directory = args.dir
            if not directory.is_dir():
                print(f'ERROR: "{directory}" is not an existing directory!')
                return
            index_directory(directory)
        else:
            # Index specific box by name
            box_name = args.box
            if not env.is_known_box(box_name):
                print(f'ERROR: Unknown box "{box_name}"')
                return

            box = env.get_box(box_name)
            if box is None:
                print(f'ERROR: Box "{box_name}" not found')
                return

            index(box)
