from typing import TYPE_CHECKING

from bead import tech

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


class CmdIndexRebuild(Command):
    '''
    Rebuild the SQLite index for a box directory.
    '''

    def declare(self, arg):
        arg('directory', type=tech.fs.Path, help='Box directory to rebuild index for')

    def run(self, args, env: 'Environment'):
        from bead.box_index import BoxIndex
        
        directory: tech.fs.Path = args.directory

        if not directory.is_dir():
            print(f'ERROR: "{directory}" is not an existing directory!')
            return

        try:
            print(f'Rebuilding index for box directory: {directory}')
            box_index = BoxIndex(directory)
            box_index.rebuild()
            print('Index rebuild completed successfully')
        except Exception as e:
            print(f'ERROR: Failed to rebuild index: {e}')
            return


def sync(box):
    '''Sync index for a single box.'''
    from bead.box_index import BoxIndex
    
    try:
        print(f'Syncing box "{box.name}" at {box.location}')
        box_index = BoxIndex(box.location)
        box_index.sync()
        print('  ✓ Success')
        return True
    except Exception as e:
        print(f'  ✗ Failed: {e}')
        return False


def sync_directory(directory):
    '''Sync index for a directory.'''
    from bead.box_index import BoxIndex
    
    try:
        print(f'Syncing directory {directory}')
        box_index = BoxIndex(directory)
        box_index.sync()
        print('  ✓ Success')
        return True
    except Exception as e:
        print(f'  ✗ Failed: {e}')
        return False


def sync_all(boxes):
    '''Sync indexes for all boxes.'''
    if not boxes:
        print('No boxes defined')
        return
    
    print(f'Syncing indexes for {len(boxes)} box(es)...')
    success_count = 0
    
    for box in boxes:
        if sync(box):
            success_count += 1
    
    print(f'Completed: {success_count}/{len(boxes)} boxes synced successfully')


class CmdIndexSync(Command):
    '''
    Sync the SQLite index for a specific box, directory, or all boxes.
    '''

    def declare(self, arg):
        arg('box_name', nargs='?', help='Box name to sync')
        arg('--dir', type=tech.fs.Path, help='Box directory to sync')
        arg('--all', action='store_true', help='Sync all boxes')

    def run(self, args, env: 'Environment'):
        # Count how many options are specified
        options_count = sum([
            bool(args.box_name),
            bool(args.dir),
            bool(args.all)
        ])
        
        if options_count == 0:
            print('ERROR: Must specify either a box name, --dir, or --all')
            return
        
        if options_count > 1:
            print('ERROR: Cannot specify more than one of: box name, --dir, --all')
            return
        
        if args.all:
            sync_all(env.get_boxes())
        elif args.dir:
            # Sync specific directory
            directory = args.dir
            if not directory.is_dir():
                print(f'ERROR: "{directory}" is not an existing directory!')
                return
            sync_directory(directory)
        else:
            # Sync specific box by name
            box_name = args.box_name
            if not env.is_known_box(box_name):
                print(f'ERROR: Unknown box "{box_name}"')
                return
            
            box = env.get_box(box_name)
            if box is None:
                print(f'ERROR: Box "{box_name}" not found')
                return
            
            sync(box)
