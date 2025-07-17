import importlib.util
from pathlib import Path
from typing import List, Tuple

from app.pipelines.NGC.config import FTPConfig, PrefectConfig


def discover_config_files(base_path: str = ".") -> List[Path]:
    """Discover all config.py files in subdirectories."""
    current_dir = Path(base_path)
    config_files = []

    # Look for subdirectories with config.py files
    for item in current_dir.iterdir():
        if item.is_dir() and not item.name.startswith("_"):
            config_path = item / "config.py"
            if config_path.exists():
                config_files.append(config_path)

    return config_files


def load_config_from_file(config_path: Path) -> Tuple["FTPConfig", "PrefectConfig"]:
    """Dynamically load FTPConfig and PrefectConfig from a config file."""
    # Create a unique module name based on the file path
    module_name = f"dynamic_config_{config_path.parent.name}"

    # Load the module dynamically
    spec = importlib.util.spec_from_file_location(module_name, config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)

    # Extract the config classes
    FTPConfig = getattr(config_module, "FTPConfig", None)
    PrefectConfig = getattr(config_module, "PrefectConfig", None)

    if not FTPConfig or not PrefectConfig:
        raise ValueError(
            f"Config file {config_path} must define both FTPConfig and PrefectConfig"
        )

    # Instantiate the configs
    return FTPConfig(), PrefectConfig()
