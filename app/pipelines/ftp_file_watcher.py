import asyncio
import ftplib
import hashlib
import re
from dataclasses import asdict
from datetime import datetime
from tempfile import SpooledTemporaryFile
from typing import Dict, List, Optional

from flow import process_csv_flow
from prefect.deployments import run_deployment

from app.db.connection import get_session

# Import your specific flow and database
from app.pipelines.NGC.config import FTPConfig, PrefectConfig
from app.sqlalchemy_schemas.file_hash import FileHashTable


class FTPFileWatcher:
    def __init__(
        self,
        ftp_config: "FTPConfig",
        prefect_config: "PrefectConfig",
        name: str = "",
        org_name: Optional[str] = None,
    ):
        self.ftp_config = ftp_config
        self.prefect_config = prefect_config
        self.name = name
        self.org_name = org_name or name  # Use name as org_name if not provided
        self.processed_files: Dict[str, str] = {}  # In-memory cache

        # Compile regex patterns for efficiency
        self.file_match_pattern_compiled = re.compile(
            self.ftp_config.file_pattern_match
        )
        self.home_file_pattern_compiled = re.compile(self.ftp_config.home_file_pattern)
        self.visiting_file_pattern_compiled = re.compile(
            self.ftp_config.visiting_file_pattern
        )

    def calculate_file_hash(self, file_data: bytes) -> str:
        """Calculate SHA-256 hash of file data"""
        return hashlib.sha256(file_data).hexdigest()

    def check_file_processed(self, file_hash: str) -> bool:
        """Check if file has been processed before by looking up its hash"""
        # First check in-memory cache
        if file_hash in self.processed_files:
            return True

        # Then check database
        with get_session() as db:
            exists = (
                db.query(FileHashTable)
                .filter(FileHashTable.sha_256_hash == file_hash)
                .first()
                is not None
            )

            if exists:
                # Add to cache to avoid repeated DB queries
                self.processed_files[file_hash] = datetime.utcnow().isoformat()

            return exists

    def record_file_processed(self, file_hash: str):
        """Record that a file has been processed"""
        with get_session() as db:
            try:
                # Check if hash already exists
                existing = (
                    db.query(FileHashTable)
                    .filter(FileHashTable.sha_256_hash == file_hash)
                    .first()
                )

                if not existing:
                    # Create new file hash record
                    file_hash_record = FileHashTable(
                        sha_256_hash=file_hash, org_name=self.org_name
                    )
                    db.add(file_hash_record)
                    db.commit()

                    # Add to in-memory cache
                    self.processed_files[file_hash] = datetime.utcnow().isoformat()
                    print(
                        f"[{self.name}] Recorded new file hash: {file_hash[:8]}... for org: {self.org_name}"
                    )
                else:
                    print(f"[{self.name}] File hash already exists: {file_hash[:8]}...")

            except Exception as e:
                print(f"[{self.name}] Failed to record file hash: {e}")
                db.rollback()
                raise

    async def trigger_flow(
        self,
        file_data: bytes,
        filename: str,
        file_hash: str,
        skip_rows: int,
        operator_name: str,
        file_type: str,
        pmn_code: str,
    ):
        """Trigger Prefect flow deployment with file data"""
        try:
            # Add source prefix to filename for tracking
            full_filename = f"{self.name}/{filename}"
            print(f"[{self.name}] Triggering flow for {filename}")

            # Convert ServiceMapping objects to dictionaries
            service_mappings_dicts = [
                asdict(mapping) for mapping in self.ftp_config.service_mappings
            ]
            self.record_file_processed(file_hash)

            await run_deployment(
                name=self.prefect_config.deployment_name,
                parameters={
                    "file_source": file_data,
                    "filename": full_filename,
                    "service_mappings": service_mappings_dicts,
                    "skip_rows": skip_rows,
                    "vpmn": operator_name,
                    "file_type": file_type,  # New parameter
                    "pmn_code": pmn_code,  # New parameter
                    "file_hash": file_hash
                },
            )
            print(f"[{self.name}] ✅ Successfully triggered flow for {filename}")

            # Record file as processed


        except Exception as e:
            print(f"[{self.name}] ❌ Failed to trigger flow for {filename}: {e}")
            raise

    def trigger_flow_direct(
        self,
        file_data: bytes,
        filename: str,
        file_hash: str,
        skip_rows: int,
        vpmn: str,
        file_type: str,
        pmn_code: str,
    ):
        """Direct flow execution using imported flow"""
        try:
            # Add source prefix to filename for tracking
            full_filename = f"{self.name}/{filename}"

            print(f"[{self.name}] 🚀 Processing file directly: {filename}")

            # Convert ServiceMapping objects to dictionaries
            service_mappings_dicts = [
                asdict(mapping) for mapping in self.ftp_config.service_mappings
            ]
            # Record file as processed
            self.record_file_processed(file_hash)
            process_csv_flow(
                file_source=file_data,
                filename=full_filename,
                service_mappings=service_mappings_dicts,
                skip_rows=skip_rows,
                vpmn=vpmn,
                file_type=file_type,
                file_hash=file_hash
            )
            print(f"[{self.name}] ✅ Successfully processed {filename}")

        except Exception as e:
            print(f"[{self.name}] ❌ Failed to process {filename}: {e}")
            raise

    def download_file(self, filename: str) -> Optional[bytes]:
        """Download file from FTP server"""
        try:
            print(f"[{self.name}] Downloading {filename}...")
            ftp = self.get_ftp_connection()

            # Use SpooledTemporaryFile to handle large files efficiently
            with SpooledTemporaryFile(
                max_size=10 * 1024 * 1024
            ) as temp_file:  # 10MB threshold
                ftp.retrbinary(f"RETR {filename}", temp_file.write)
                temp_file.seek(0)
                file_data = temp_file.read()

            ftp.quit()
            print(f"[{self.name}] Downloaded {filename} ({len(file_data)} bytes)")
            return file_data

        except Exception as e:
            print(f"[{self.name}] Failed to download {filename}: {e}")
            return None

    async def process_file(
        self, filename: str, skip_rows: int
    ):
        """Process a single file"""
        # Check if the filename matches the _MFS_ pattern
        if not self.file_match_pattern_compiled.match(filename):
            print(
                f"[{self.name}] File {filename} does not contain {self.ftp_config.file_pattern_match}, skipping."
            )
            return

        file_data = self.download_file(filename)
        if file_data:
            # Calculate hash to check if already processed
            file_hash = self.calculate_file_hash(file_data)

            if self.check_file_processed(file_hash):
                print(
                    f"[{self.name}] File {filename} (hash: {file_hash[:8]}...) already processed, skipping"
                )
                return

            vpmn = filename.split("_")[self.ftp_config.operator_location_in_file_name]

            # Determine file type (home/visiting)
            file_type = "unknown"
            if self.home_file_pattern_compiled.search(filename):
                file_type = "home"
            elif self.visiting_file_pattern_compiled.search(filename):
                file_type = "visiting"
            else:
                print(
                    f"[{self.name}] Warning: Could not determine file type for {filename}"
                )

            # Extract PMN code
            filename_parts = filename.split("_")
            pmn_code = ""
            if len(filename_parts) > self.ftp_config.pmn_code_location_in_file_name:
                pmn_code_raw = filename_parts[
                    self.ftp_config.pmn_code_location_in_file_name
                ]
                pmn_code = pmn_code_raw[: self.ftp_config.pmn_code_length]
            else:
                print(
                    f"[{self.name}] Warning: Could not extract PMN code from {filename}. Check pmn_code_location_in_file_name."
                )

            if self.prefect_config.use_direct_execution:
                self.trigger_flow_direct(
                    file_data, filename, file_hash, skip_rows, vpmn, file_type, pmn_code
                )
            else:
                await self.trigger_flow(
                    file_data, filename, file_hash, skip_rows, vpmn, file_type, pmn_code
                )

    def get_ftp_connection(self) -> ftplib.FTP:
        """Establish FTP connection"""
        ftp = ftplib.FTP()
        ftp.connect(self.ftp_config.host, self.ftp_config.port)
        ftp.login(self.ftp_config.user, self.ftp_config.password)
        if self.ftp_config.remote_dir:
            ftp.cwd(self.ftp_config.remote_dir)
        return ftp

    def list_csv_files(self) -> List[str]:
        """List all CSV files in the FTP directory with debug info"""
        try:
            ftp = self.get_ftp_connection()

            # Get current working directory
            current_dir = ftp.pwd()
            print(f"[{self.name}] Current FTP directory: {current_dir}")

            # List all files (not just CSV)
            all_files = []
            ftp.retrlines("NLST", all_files.append)

            print(f"[{self.name}] Total files in directory: {len(all_files)}")

            # Show first few files for debugging
            if all_files:
                print(f"[{self.name}] Sample files:")
                for f in all_files[:5]:
                    print(f"[{self.name}]   - {f}")
                if len(all_files) > 5:
                    print(f"[{self.name}]   ... and {len(all_files) - 5} more files")

            ftp.quit()

            # Filter for CSV files
            csv_files = [f for f in all_files if f.lower().endswith(".csv")]

            if not csv_files and all_files:
                # Show file extensions present
                extensions = set(
                    f.split(".")[-1].lower() for f in all_files if "." in f
                )
                print(f"[{self.name}] File extensions found: {', '.join(extensions)}")

            return csv_files

        except Exception as e:
            print(f"[{self.name}] Failed to list files: {e}")
            return []

    async def watch(self):
        """Main watching loop"""
        # Immediate print to confirm method is called
        print(f"[DEBUG] watch() called for {self.name}")

        try:
            print(f"[{self.name}] Starting FTP watcher for {self.ftp_config.host}")
            print(f"[{self.name}] Organization: {self.org_name}")
        except Exception as e:
            print(f"[ERROR] Failed to log initial messages: {e}")
            raise
        print(f"[{self.name}] 🔍 Starting FTP file watcher...")
        print(f"[{self.name}] Config: {self.ftp_config.host}:{self.ftp_config.port}")
        print(f"[{self.name}] Deployment: {self.prefect_config.deployment_name}")
        print(f"[{self.name}] Poll interval: {self.ftp_config.poll_interval}s")
        print(
            f"[{self.name}] Direct execution: {self.prefect_config.use_direct_execution}"
        )
        print(f"[{self.name}] Home file pattern: {self.ftp_config.home_file_pattern}")
        print(
            f"[{self.name}] Visiting file pattern: {self.ftp_config.visiting_file_pattern}"
        )
        print(
            f"[{self.name}] PMN code location: {self.ftp_config.pmn_code_location_in_file_name}"
        )
        print(f"[{self.name}] PMN code length: {self.ftp_config.pmn_code_length}")

        # Add connection test
        print(f"[{self.name}] Testing FTP connection...")
        try:
            ftp = self.get_ftp_connection()
            ftp.quit()
            print(f"[{self.name}] ✅ FTP connection successful")
        except Exception as e:
            print(f"[{self.name}] ❌ FTP connection failed: {e}")
            return

        while True:
            try:
                print(f"[{self.name}] Starting poll cycle...")
                csv_files = self.list_csv_files()
                print(f"[{self.name}] Found {len(csv_files)} CSV files")

                for filename in csv_files:
                    print(f"[{self.name}] Processing {filename}")
                    # Only pass skip_rows, as file_match_pattern is now a class attribute
                    await self.process_file(filename, self.ftp_config.skip_rows)

                print(
                    f"[{self.name}] Sleeping for {self.ftp_config.poll_interval} seconds..."
                )
                await asyncio.sleep(self.ftp_config.poll_interval)

            except Exception as e:
                print(f"[{self.name}] Watch loop error: {e}")
