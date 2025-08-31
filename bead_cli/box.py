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
