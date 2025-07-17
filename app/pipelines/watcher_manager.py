import asyncio
from pathlib import Path
from typing import Dict

from config_loader import discover_config_files, load_config_from_file
from ftp_file_watcher import FTPFileWatcher


class FTPWatcherManager:
    """
    Manages the lifecycle of multiple FTP file watchers.

    This class is responsible for discovering configuration files,
    initializing `FTPFileWatcher` instances based on those configurations,
    and running all initialized watchers concurrently.
    """

    def __init__(self):
        """
        Initializes the FTPWatcherManager.

        The `watchers` attribute is a dictionary that will store
        initialized `FTPFileWatcher` instances, with their names as keys.
        """
        self.watchers: Dict[str, FTPFileWatcher] = {}

    def initialize_watchers(self):
        """
        Discovers configuration files and initializes FTP watchers.

        This method scans the current directory for `config.py` files within
        subdirectories. For each discovered `config.py`, it loads the FTP and Prefect
        configurations and then creates an `FTPFileWatcher` instance.
        Initialized watchers are stored in the `self.watchers` dictionary.
        If a watcher fails to initialize, an error message is printed.
        """
        base_path = Path(__file__).parent
        config_files = discover_config_files(base_path)

        for config_path in config_files:
            try:
                watcher_name = config_path.parent.name
                # Load configs
                ftp_config, prefect_config = load_config_from_file(config_path)

                # Create watcher
                watcher = FTPFileWatcher(
                    ftp_config=ftp_config,
                    prefect_config=prefect_config,
                    name=watcher_name,
                    org_name=ftp_config.org_name,
                )
                self.watchers[watcher_name] = watcher

                print(f"Initialized watcher: {watcher_name}")
                print(f"  - FTP: {ftp_config.host}:{ftp_config.port}")
                print(f"  - Deployment: {prefect_config.deployment_name}")

            except Exception as e:
                print(f"Failed to initialize watcher for {config_path}: {e}")

    async def run_all_watchers(self):
        """
        Runs all initialized FTP watchers concurrently.

        This asynchronous method creates an asyncio task for each `FTPFileWatcher`
        instance stored in `self.watchers` and runs them concurrently using `asyncio.gather`.
        It prints progress messages and also catches exceptions that might occur
        within individual watcher tasks to prevent silent failures, logging any
        exceptions found.
        """
        if not self.watchers:
            print("No watchers configured!")
            return

        print(f"Creating tasks for {len(self.watchers)} watchers...")

        tasks = []
        for name, watcher in self.watchers.items():
            print(f"Creating task for watcher: {name}")
            task = asyncio.create_task(watcher.watch(), name=f"watch-{name}")
            tasks.append(task)
            print(f"Task created: {task}")

        print(f"Starting {len(tasks)} tasks...")

        try:
            # Add return_exceptions=True to see if any task is failing silently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for any exceptions
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"Task {i} failed with exception: {result}")

        except Exception as e:
            print(f"Error in gather: {e}")
            raise