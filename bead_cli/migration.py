"""
Configuration migration utilities for transitioning to new directory structure.
"""

import shutil
import platformdirs
from bead.infra.fs import Path


def get_legacy_dir() -> Path:
    """Get the old config directory using legacy app name"""
    return Path(platformdirs.user_config_dir('bead_cli-6a4d9d98-8e64-4a2a-b6c2-8a753ea61daf'))


def migrate_config_if_needed(config_dir: Path):
    """Copy legacy config to new location if needed"""
    legacy_dir = get_legacy_dir()

    if legacy_dir.exists() and not (config_dir / '.migrated').exists():
        print("Migrating configuration to standard directories...")
        config_dir.mkdir(parents=True, exist_ok=True)

        if (legacy_dir / 'env.json').exists():
            shutil.copy2(legacy_dir / 'env.json', config_dir / 'boxes.json')

        (config_dir / '.migrated').touch()
        print(f"Configuration migrated to {config_dir}")
        print("Your old configuration remains intact for compatibility")