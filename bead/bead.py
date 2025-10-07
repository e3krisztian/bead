from abc import ABCMeta
from abc import abstractmethod
from typing import Sequence

from .exceptions import InvalidArchive
from .meta import BeadName
from .meta import InputSpec
from .infra.timestamp import time_from_timestamp


class Computation:
    '''
    Core identity of a computation.

    Like a recipe that defines what dish you're making (kind),
    what it's called (name), and what ingredients it needs (inputs).

    Both active workspaces and frozen beads are instances of a computation.
    '''

    # high level view of computation
    # Subclasses may implement these as properties or plain attributes
    kind: str
    name: BeadName
    inputs: Sequence[InputSpec]

    def get_input(self, name) -> InputSpec:
        for input in self.inputs:
            if name == input.name:
                return input
        raise LookupError(f'Input "{name}" not found')


class Bead(Computation):
    '''
    Frozen snapshot of a computation with provenance.

    Like a finished dish with a timestamp and location tracking.
    Immutable record of running a computation (code + inputs → output)
    with content_id for verification and freeze_time for history.

    The tuple (box_name, name, content_id) uniquely identifies a bead
    and is sufficient to resolve it to an Archive.

    Note: content_id guarantees same data content, but beads with same
    content_id can have different metadata (box_name, name) or reference
    inputs differently.
    '''

    # frozen beads only details
    # Subclasses may implement these as properties or plain attributes
    content_id: str
    freeze_time_str: str
    box_name: str

    @property
    def freeze_time(self):
        return time_from_timestamp(self.freeze_time_str)


class Archive(Bead, metaclass=ABCMeta):
    '''
    Provide high-level access to content of a bead.
    '''

    def unpack_to(self, workspace):
        self.unpack_code_to(workspace.directory)
        workspace.create_directories()
        self.unpack_meta_to(workspace)

    @abstractmethod
    def unpack_data_to(self, fs_dir):
        pass

    @abstractmethod
    def unpack_code_to(self, fs_dir):
        pass

    @abstractmethod
    def unpack_meta_to(self, workspace):
        pass

    @abstractmethod
    def validate(self):
        raise InvalidArchive

    @property
    @abstractmethod
    def location(self) -> str:
        ...
