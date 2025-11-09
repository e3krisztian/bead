"""Centralized test helpers for bead CLI testing.

This module provides reusable helper functions for creating test beads
and managing test data in a consistent way across all test files.
"""

import glob
from bead.infra.fs import rmtree
from bead.infra.fs import write_file
from bead.workspace import Workspace
from bead.ziparchive import ZipArchive


def create_bead_family(box, bead_name, timestamps, tmp_path_factory, kind='KIND:test'):
    """Create family of beads with SAME name but different timestamps.

    This creates a version history for a single bead identity.

    Args:
        box: Target box for storage
        bead_name: The name ALL beads in the family will share
        timestamps: List of timestamps for version history
        tmp_path_factory: pytest fixture for temp directories
        kind: Bead kind identifier (string)

    IMPORTANT: About bead 'kind':
    - In real usage, 'kind' is typically an auto-generated UUID (e.g., "a1b2c3d4-...")
    - Each new bead gets a unique kind UUID when created with 'bead new'
    - Different bead names usually have different kinds
    - Same bead name can have multiple kinds over time (lineage splits)
    - Test usage of simple strings like 'KIND:test' is just for readability
    - The kind determines logical data compatibility, not just the name
    """
    for timestamp in timestamps:
        workspace_dir = tmp_path_factory.mktemp('bead_family') / bead_name
        ws = Workspace(workspace_dir)  # ws.name becomes bead_name
        ws.create(kind)

        # Create content with both bead name and timestamp for uniqueness
        content = f'{bead_name}_{timestamp}'
        write_file(ws.directory / 'README', content)
        write_file(ws.directory / 'output/README', content)

        # Create sentinel files for tests that expect them
        sentinel_file = ws.directory / f'sentinel-{timestamp}'
        write_file(sentinel_file, timestamp)

        box.store(ws, timestamp)  # All stored with same bead_name
        rmtree(workspace_dir)  # Clean up workspace


def get_bead_archive(box, bead_name, timestamp):
    """Get ZipArchive object from box by bead name and timestamp with EXACT matching.

    Args:
        box: The box containing the bead
        bead_name: Name of the bead to retrieve
        timestamp: Timestamp of the specific version

    Returns:
        ZipArchive object for the bead

    Raises:
        ValueError: If no exact match or multiple matches found
    """
    pattern = str(box.directory / f'{bead_name}_*.zip')
    bead_files = glob.glob(pattern)

    # EXACT timestamp matching only - fail if no/multiple matches
    exact_matches = [f for f in bead_files if timestamp in f]

    if len(exact_matches) == 0:
        raise ValueError(f"No bead archive found for {bead_name} with timestamp {timestamp}")
    if len(exact_matches) > 1:
        raise ValueError(f"Multiple bead archives found for {bead_name} with timestamp {timestamp}: {exact_matches}")

    return ZipArchive(exact_matches[0])


def create_bead_with_inputs(shell, box, bead_name, inputs, timestamp, tmp_path_factory, kind='KIND:test'):
    """Create a bead with inputs, similar to the bead_with_inputs fixture.

    Args:
        shell: Test shell for bead commands
        box: Target box for storage
        bead_name: Name of the bead to create
        inputs: Dict mapping input names to bead names
        timestamp: Timestamp for the bead
        tmp_path_factory: pytest fixture for temp directories
        kind: Bead kind identifier

    Returns:
        The stored bead archive (ZipArchive object)
    """
    shell.bead('new', bead_name)
    shell.cd(bead_name)
    content = f'{bead_name}_{timestamp}'
    shell.write_file('README', content)
    shell.write_file('output/README', content)

    # Add inputs
    for input_name, input_bead in inputs.items():
        shell.bead('input', 'add', input_name, input_bead)

    with shell.environment:
        archive = box.store(Workspace('.'), timestamp)

    shell.cd('..')
    shell.bead('discard', bead_name)

    # Return the archive so tests can access its properties
    return ZipArchive(archive)


def split_status_by_inputs(status_output):
    """Split status output into dict keyed by input name.

    Args:
        status_output: String output from `bead status` command

    Returns:
        Dict mapping input names to their section text
        Example: {'producer': 'input/producer\\n\\tFrom: ...', 'dataset': 'input/dataset\\n\\t...'}
    """
    sections = {}
    lines = status_output.split('\n')
    current_input = None
    current_section = []

    for line in lines:
        if line.startswith('input/'):
            # Save previous section if any
            if current_input:
                sections[current_input] = '\n'.join(current_section)
            # Start new section
            current_input = line[6:]  # Remove 'input/' prefix
            current_section = [line]
        elif current_input and (line.startswith('\t') or line.strip() == ''):
            # Part of current input section
            current_section.append(line)
        elif current_input:
            # End of inputs section
            sections[current_input] = '\n'.join(current_section)
            current_input = None
            current_section = []

    # Save last section if any
    if current_input:
        sections[current_input] = '\n'.join(current_section)

    return sections


