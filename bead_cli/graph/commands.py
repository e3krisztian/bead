import argparse
import os
import subprocess
import textwrap
from typing import TYPE_CHECKING
from typing import Set
import webbrowser

from bead.box import search
from bead.infra.fs import Path
from bead.infra.fs import write_file

from . import sketch as graph_sketch
from ..cmdparse import Command
from ..common import die
from .dummy import Dummy
from .io import read_beads
from .io import write_beads
from .sketch import Sketch

if TYPE_CHECKING:
    from ..environment import Environment


class CmdGraph(Command):
    '''
    Visualize dependency graph and connections between beads.

    This command processes a graph of beads through a pipeline of sub-commands.
    Each sub-command transforms the graph and passes it to the next command.

    By default, the pipeline starts by scanning all enabled boxes and loading
    all beads. Use "load" to skip scanning and start from a saved graph.

    PIPELINE COMMANDS:

    load <filename.web>
        Load previously saved graph from file (skips box scanning)

    / [source-name[s]] .. [sink-name[s]] /
        Filter by following input connections:
        - sources: keep only beads reachable from these clusters
        - sinks: keep only beads that lead to these clusters
        - use ".." to separate sources from sinks
        - use "/" to end the filter expression

    save <filename.web>
        Save current graph metadata to file for later reuse

    dot <filename.dot>
        Export graph in GraphViz DOT format

    png <filename.png>
        Render graph as PNG image (requires graphviz)

    svg <filename.svg>
        Render graph as SVG image (requires graphviz)

    color
        Assign freshness colors based on input versions:
        - green: all inputs are up-to-date
        - orange: some inputs are outdated
        - grey: superseded by newer version
        - red: phantom (referenced but missing)

    heads
        Show only the most recent beads per cluster, plus any older
        beads that are still referenced by outdated computations

    view <filename>
        Open file in browser

    EXAMPLES:

    # Visualize all beads
    bead graph dot all.dot

    # Create colored PNG of all beads
    bead graph color png overview.png

    # Show only latest versions
    bead graph heads color svg latest.svg

    # Filter: show path from data_input to final_report
    bead graph / data_input .. final_report / color png path.png

    # Fast workflow: cache expensive scan, then filter
    bead graph save cache.web
    bead graph load cache.web / analysis .. / heads png filtered.png

    # Multiple filters: sources AND sinks
    bead graph / raw_data processed_data .. / / .. final_output / color svg result.svg

    # View in browser after creating
    bead graph heads color svg latest.svg view latest.svg
    '''

    FORMATTER_CLASS = argparse.RawDescriptionHelpFormatter

    def declare(self, arg):
        arg(
            'words',
            metavar='...',
            nargs=argparse.REMAINDER,
            help='Sub-commands and their arguments'
        )

    def run(self, args, env: 'Environment'):
        if not args.words:
            print('No sub-commands given, see usage below:')
            print(textwrap.dedent(self.__doc__ or ''))
            return

        commands, remaining_words = parse_commands(env, args.words)
        if remaining_words:
            msg = 'ERROR: Could not fully parse command line.\n'
            if commands:
                msg += '\nSuccessfully parsed:\n'
                for cmd in commands:
                    msg += f'  - {cmd}\n'
            msg += f'\nFailed to parse: {remaining_words}\n'
            msg += '\nValid sub-commands: load, save, dot, png, svg, /, color, heads, view'
            msg += '\nRun "bead graph --help" for examples and documentation.'
            die(msg)

        sketch = Sketch.from_beads([])
        for command in commands:
            sketch = command(sketch)


def parse_commands(env, words):
    remaining_words = words[::-1]
    commands = []

    if remaining_words and remaining_words[-1] != 'load':
        commands.append(LoadAll(env.get_boxes()))

    while remaining_words:
        remaining = remaining_words[:]
        cmd_name = remaining_words.pop()
        try:
            cmd_class = SUBCOMMANDS[cmd_name]
            cmd = cmd_class(remaining_words)
        except:
            return commands, remaining[::-1]
        commands.append(cmd)

    return commands, remaining_words[::-1]


class SketchProcessor:
    def __init__(self, _args):
        pass

    def __call__(self, sketch):
        return sketch

    def __str__(self):
        cls = self.__class__.__name__
        args = vars(self)
        return f'{cls}({args})'

    def sketch_from_beads(self, beads):
        return Sketch.from_beads([Dummy.from_bead(bead) for bead in beads])


class ProcessorWithFileName(SketchProcessor):
    def __init__(self, args):
        self.file_name = Path(args.pop())


class LoadAll(SketchProcessor):
    def __init__(self, boxes):
        super().__init__([])
        self.boxes = boxes

    def __call__(self, _sketch):
        beads = load_all_beads(self.boxes)
        print(f"Loaded {len(beads)} beads")
        return self.sketch_from_beads(beads)


class Load(ProcessorWithFileName):
    def __call__(self, _sketch):
        beads = read_beads(self.file_name)
        return self.sketch_from_beads(beads)


class Save(ProcessorWithFileName):
    def __call__(self, sketch):
        write_beads(self.file_name, sketch.beads)
        return sketch


class WriteDot(ProcessorWithFileName):
    def __call__(self, sketch):
        dot_str = sketch.as_dot()
        write_file(self.file_name, dot_str)
        return sketch


class WritePng(ProcessorWithFileName):
    def __call__(self, sketch):
        dot_str = sketch.as_dot()
        print(f"Creating PNG: {self.file_name}")
        graphviz_dot(dot_str, self.file_name, format='png')
        return sketch


class WriteSvg(ProcessorWithFileName):
    def __call__(self, sketch):
        dot_str = sketch.as_dot()
        print(f"Creating SVG: {self.file_name}")
        graphviz_dot(dot_str, self.file_name, format='svg')
        return sketch


class View(ProcessorWithFileName):
    def __call__(self, sketch):
        print(f"Viewing {self.file_name}")
        webbrowser.open(self.file_name.as_posix())


class Filter(SketchProcessor):
    def __init__(self, args):
        self.sources = self._pop_names(args, sentinel='..')
        self.sinks = self._pop_names(args, sentinel='/')
        super().__init__(args)

    def _pop_names(self, args, sentinel) -> Set[str]:
        names: Set[str] = set()
        while args:
            name = args.pop()
            if name == sentinel:
                return names
            if name in ('..', '/'):
                raise ValueError(f'Unexpected delimiter: {repr(name)} after {names}.')
            if not is_valid_name(name):
                raise ValueError(f'Malformed name: {repr(name)} after {names}.')
            names.add(name)
        raise ValueError(f'Delimiter not found: {repr(sentinel)}.')

    def __call__(self, sketch):
        if self.sources:
            sketch = graph_sketch.set_sources(sketch, self.sources)
        if self.sinks:
            sketch = graph_sketch.set_sinks(sketch, self.sinks)
        return sketch


def is_valid_name(name):
    return name not in ('..', '/')


class SetFreshness(SketchProcessor):
    def __call__(self, sketch):
        sketch.color_beads()
        return sketch


class KeepOnlyHeads(SketchProcessor):
    def __call__(self, sketch):
        return graph_sketch.heads_of(sketch).drop_deleted_inputs()


SUBCOMMANDS = {
    'load': Load,
    'save': Save,
    'dot': WriteDot,
    'png': WritePng,
    'svg': WriteSvg,
    '/': Filter,
    'color': SetFreshness,
    'heads': KeepOnlyHeads,
    'view': View,
}


def load_all_beads(boxes):
    columns = int(os.environ.get('COLUMNS', 80))
    all_beads = []
    for n, bead in enumerate(search(boxes).all()):
        msg = f"\rLoaded bead {n + 1} ({bead.box_name} : {bead.name} @ {bead.freeze_time_str})"[:columns]
        msg = msg + ' ' * (columns - len(msg))
        print(msg, end="", flush=True)
        all_beads.append(bead)
    print("\r" + " " * columns + "\r", end="")
    return all_beads


def graphviz_dot(dot_str, output_file, format):
    cmd = ['dot', '-o', output_file, '-T', format]
    subprocess.run(cmd, input=dot_str.encode('utf-8'), capture_output=True, check=True)
