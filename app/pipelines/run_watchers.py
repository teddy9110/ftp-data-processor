import asyncio
import logging
import sys
from pathlib import Path

# Add the pipelines directory to the Python path
sys.path.insert(0, str(Path(__file__).parent))

from watcher_manager import FTPWatcherManager



async def main():
    """
      Main asynchronous function to set up, initialize, and run the FTP Watcher Manager.

      This function performs the following steps:
      1. Configures basic logging for the application.
      2. Initializes an instance of `FTPWatcherManager`.
      3. Discovers and initializes all FTP watchers based on available configurations.
      4. If no watchers are found, it prints an error message and exits.
      5. If watchers are found, it prints a confirmation message.
      6. Runs all initialized watchers asynchronously.
      7. Includes error handling for any fatal exceptions that occur during the watcher execution.
      """

    logging.basicConfig(
        level=logging.DEBUG,  # Changed from INFO to DEBUG
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    print("🚀 Starting FTP Watcher Manager...")

    # Initialize the manager
    manager = FTPWatcherManager()

    # Discover and initialize all watchers
    manager.initialize_watchers()

    if not manager.watchers:
        print(
            "❌ No watchers found! Make sure you have config.py files in subdirectories."
        )
        return

    print(f"\n✅ Found {len(manager.watchers)} watcher(s)")
    print("=" * 50)

    try:
        # Run all watchers
        await manager.run_all_watchers()
    except Exception as e:
        logging.error(
            f"Fatal error in watcher manager: {e}",
        )
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down FTP watchers...")
        print("Goodbye! 👋")
