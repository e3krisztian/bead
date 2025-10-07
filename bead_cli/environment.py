'''
User specific environment
'''

import os
from dataclasses import dataclass

from bead.box import Box
from bead.infra import persistence
from bead.infra.fs import Path

ENV_BOXES = 'boxes'
BOX_NAME = 'name'
BOX_LOCATION = 'directory'
BOX_ENABLED = 'enabled'


@dataclass
class MetaBox:
    """Lightweight box representation without heavy Box object initialization."""
    name: str
    directory: Path
    index_path: Path
    enabled: bool

    def create_box(self) -> Box:
        """Create the actual Box object when needed."""
        return Box(self.name, self.directory, self.index_path, self.enabled)

    def remove_index(self):
        """Remove index file (e.g., for reindexing)."""
        if self.index_path.exists():
            self.index_path.unlink()


class Environment:
    """
    This class is responsible for storing/retrieving user specific data.

    It has the list of boxes and their definitions.
    It can also store box specific information (e.g. an index).
    """

    def __init__(self, config_dir: Path, state_dir: Path):
        self.config_dir = Path(config_dir)
        self.state_dir = Path(state_dir)
        self._content = {}
        # Ensure directories exist
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        if os.path.exists(self.config_file):
            self.load()

    @property
    def config_file(self):
        return self.config_dir / 'boxes.json'

    def load(self):
        with open(self.config_file) as f:
            self._content = persistence.load(f)

    def save(self):
        with open(self.config_file, 'w') as f:
            persistence.dump(self._content, f)

    def get_box_index_path(self, box_name: str) -> Path:
        """Calculate index file path for a box in state directory."""
        index_filename = f"box_index_{box_name}.sqlite"
        return self.state_dir / index_filename

    def get_all_boxes(self):
        def box(box_spec):
            name = box_spec.get(BOX_NAME)
            location = Path(box_spec.get(BOX_LOCATION))
            enabled = box_spec.get(BOX_ENABLED, True)
            index_path = self.get_box_index_path(name)
            return Box(name, location, index_path, enabled)
        return [box(spec) for spec in self._content.get(ENV_BOXES, ())]

    def get_boxes(self):
        return [box for box in self.get_all_boxes() if box.enabled]

    def get_meta_boxes(self):
        """Get enabled meta boxes without initializing heavy Box objects."""
        return [
            MetaBox(
                name=spec.get(BOX_NAME),
                directory=Path(spec.get(BOX_LOCATION)),
                index_path=self.get_box_index_path(spec.get(BOX_NAME)),
                enabled=spec.get(BOX_ENABLED, True)
            )
            for spec in self._content.get(ENV_BOXES, [])
            if spec.get(BOX_ENABLED, True)  # Only include enabled boxes
        ]

    def set_boxes(self, boxes):
        self._content[ENV_BOXES] = [
            {
                BOX_NAME: box.name,
                BOX_LOCATION: box.location.as_posix(),
                BOX_ENABLED: box.enabled
            }
            for box in boxes]

    def add_box(self, name, directory: Path):
        boxes = self.get_all_boxes()
        # check unique box
        for box in boxes:
            if box.name == name:
                raise ValueError(f'Box with name {name} already exists')
            if box.location == directory:
                raise ValueError(
                    f'Box with location {box.location} already exists')

        index_path = self.get_box_index_path(name)
        self.set_boxes(boxes + [Box(name, directory, index_path, enabled=True)])

    def forget_box(self, name):
        # Remove the index file for this box
        index_path = self.get_box_index_path(name)
        try:
            index_path.unlink()
        except OSError:
            pass  # Ignore errors during cleanup
        
        self.set_boxes(
            box
            for box in self.get_all_boxes()
            if box.name != name)

    def get_box(self, name):
        '''
        Return box having :name or raise LookupError if not found.
        '''
        for box in self.get_all_boxes():
            if box.name == name:
                return box
        raise LookupError(f'Box not found: {name}')

    def is_known_box(self, name):
        try:
            self.get_box(name)
            return True
        except LookupError:
            return False

    def _set_box_enabled(self, name, is_enabled):
        boxes = self.get_all_boxes()
        for box in boxes:
            if box.name == name:
                box.enabled = is_enabled
                self.set_boxes(boxes)
                return
        raise ValueError(f'Box {name} not found')

    def enable_box(self, name):
        self._set_box_enabled(name, True)

    def disable_box(self, name):
        self._set_box_enabled(name, False)

