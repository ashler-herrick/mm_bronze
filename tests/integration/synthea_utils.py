"""
Utilities for working with synthea dataset in integration tests.
"""

import random
from pathlib import Path
from typing import List, Dict, Any


SYNTHEA_ROOT = Path("/home/ashler/Documents/synthea_coherent_data")


class SyntheaTestData:
    """Utility class for selecting and managing synthea test data."""

    def __init__(self):
        self.fhir_dir = SYNTHEA_ROOT / "fhir"
        self.dicom_dir = SYNTHEA_ROOT / "dicom"
        self.dna_dir = SYNTHEA_ROOT / "dna"
        self.csv_dir = SYNTHEA_ROOT / "csv"

    def get_fhir_files(self, count: int = 5, min_size: int = 0, max_size: int = float("inf")) -> List[Path]:
        """Get a selection of FHIR bundle files for API testing.

        Args:
            count: Number of files to return
            min_size: Minimum file size in bytes
            max_size: Maximum file size in bytes

        Returns:
            List of Path objects for selected FHIR files
        """
        if not self.fhir_dir.exists():
            return []

        all_files = list(self.fhir_dir.glob("*.json"))

        # Filter by size if specified
        if min_size > 0 or max_size < float("inf"):
            filtered_files = []
            for file_path in all_files:
                try:
                    size = file_path.stat().st_size
                    if min_size <= size <= max_size:
                        filtered_files.append(file_path)
                except OSError:
                    continue
            all_files = filtered_files

        # Return random selection
        return random.sample(all_files, min(count, len(all_files)))

    def get_dicom_files(self, count: int = 3) -> List[Path]:
        """Get a selection of DICOM files for SFTP testing.

        Args:
            count: Number of files to return

        Returns:
            List of Path objects for selected DICOM files
        """
        if not self.dicom_dir.exists():
            return []

        all_files = list(self.dicom_dir.glob("*.dcm"))
        return random.sample(all_files, min(count, len(all_files)))

    def get_dna_csv_files(self, count: int = 3) -> List[Path]:
        """Get a selection of DNA CSV files for SFTP testing.

        Args:
            count: Number of files to return

        Returns:
            List of Path objects for selected DNA CSV files
        """
        if not self.dna_dir.exists():
            return []

        all_files = list(self.dna_dir.glob("*.csv"))
        return random.sample(all_files, min(count, len(all_files)))

    def get_regular_csv_files(self, count: int = 2) -> List[Path]:
        """Get a selection of regular CSV files for SFTP testing.

        Args:
            count: Number of files to return

        Returns:
            List of Path objects for selected regular CSV files
        """
        if not self.csv_dir.exists():
            return []

        all_files = list(self.csv_dir.glob("*.csv"))
        return random.sample(all_files, min(count, len(all_files)))

    def get_file_info(self, file_path: Path) -> Dict[str, Any]:
        """Get information about a file for testing.

        Args:
            file_path: Path to the file

        Returns:
            Dictionary with file information
        """
        try:
            stat = file_path.stat()
            return {
                "path": str(file_path),
                "name": file_path.name,
                "size": stat.st_size,
                "extension": file_path.suffix.lstrip(".") or "bin",
                "type": self._classify_file_type(file_path),
            }
        except OSError:
            return {}

    def _classify_file_type(self, file_path: Path) -> str:
        """Classify file type for ingestion endpoint."""
        suffix = file_path.suffix.lower()

        if suffix == ".json":
            return "json"
        elif suffix == ".dcm":
            return "binary"
        elif suffix == ".csv":
            return "text"
        else:
            return "binary"

    def create_api_test_cases(self, count: int = 5, max_size: int = None) -> List[Dict[str, Any]]:
        """Create test cases for API ingestion.

        Automatically filters files to fit within Kafka message size limits after base64 encoding.

        Args:
            count: Number of test cases to create
            max_size: Optional override for maximum file size in bytes.
                     If None, uses 75% of Kafka max message size (default: ~7.5MB)

        Returns:
            List of test case dictionaries
        """
        if max_size is None:
            # Calculate effective max size accounting for base64 encoding overhead (~33%)
            # and JSON envelope overhead. Use 75% of Kafka max message size as safe limit.
            # Default Kafka max is 10MB, so effective limit is ~7.5MB
            kafka_max_message_size = 10485760  # 10MB default from config
            max_size = int(kafka_max_message_size * 0.75)

        fhir_files = self.get_fhir_files(count, max_size=max_size)
        test_cases = []

        for file_path in fhir_files:
            file_info = self.get_file_info(file_path)
            if file_info:
                # For FHIR bundles, we know they're JSON format
                test_case = {
                    "file_path": file_path,
                    "file_info": file_info,
                    "endpoint": "http://localhost:8000/ingest/json/fhir/r4/bundle",
                    "content_type": "application/json",
                    "description": f"FHIR bundle: {file_info['name']} ({file_info['size']:,} bytes)",
                }
                test_cases.append(test_case)

        return test_cases

    def create_sftp_test_cases(self, dicom_count: int = 2, csv_count: int = 3) -> List[Dict[str, Any]]:
        """Create test cases for SFTP ingestion.

        Args:
            dicom_count: Number of DICOM files to include
            csv_count: Number of CSV files to include

        Returns:
            List of test case dictionaries
        """
        test_cases = []

        # Add DICOM files
        dicom_files = self.get_dicom_files(dicom_count)
        for file_path in dicom_files:
            file_info = self.get_file_info(file_path)
            if file_info:
                test_case = {
                    "file_path": file_path,
                    "file_info": file_info,
                    "description": f"DICOM file: {file_info['name']} ({file_info['size']:,} bytes)",
                    "expected_content_type": "binary",
                }
                test_cases.append(test_case)

        # Add DNA CSV files
        dna_files = self.get_dna_csv_files(csv_count // 2)
        for file_path in dna_files:
            file_info = self.get_file_info(file_path)
            if file_info:
                test_case = {
                    "file_path": file_path,
                    "file_info": file_info,
                    "description": f"DNA CSV: {file_info['name']} ({file_info['size']:,} bytes)",
                    "expected_content_type": "text",
                }
                test_cases.append(test_case)

        # Add regular CSV files
        regular_files = self.get_regular_csv_files(csv_count - csv_count // 2)
        for file_path in regular_files:
            file_info = self.get_file_info(file_path)
            if file_info:
                test_case = {
                    "file_path": file_path,
                    "file_info": file_info,
                    "description": f"CSV data: {file_info['name']} ({file_info['size']:,} bytes)",
                    "expected_content_type": "text",
                }
                test_cases.append(test_case)

        return test_cases

    def get_mixed_size_files(self) -> Dict[str, List[Path]]:
        """Get files categorized by size for performance testing.

        Returns:
            Dictionary with size categories and file lists
        """
        result = {
            "small": [],  # < 1MB
            "medium": [],  # 1-5MB
            "large": [],  # > 5MB
        }

        fhir_files = list(self.fhir_dir.glob("*.json")) if self.fhir_dir.exists() else []

        for file_path in fhir_files:
            try:
                size = file_path.stat().st_size
                if size < 1_000_000:
                    result["small"].append(file_path)
                elif size < 5_000_000:
                    result["medium"].append(file_path)
                else:
                    result["large"].append(file_path)
            except OSError:
                continue

        return result


def is_synthea_available() -> bool:
    """Check if synthea dataset is available."""
    return SYNTHEA_ROOT.exists() and (SYNTHEA_ROOT / "fhir").exists()


def get_test_data_stats() -> Dict[str, int]:
    """Get statistics about available test data."""
    stats = {
        "fhir_files": 0,
        "dicom_files": 0,
        "dna_csv_files": 0,
        "regular_csv_files": 0,
    }

    if not SYNTHEA_ROOT.exists():
        return stats

    for key, pattern in [
        ("fhir_files", "fhir/*.json"),
        ("dicom_files", "dicom/*.dcm"),
        ("dna_csv_files", "dna/*.csv"),
        ("regular_csv_files", "csv/*.csv"),
    ]:
        try:
            files = list(SYNTHEA_ROOT.glob(pattern))
            stats[key] = len(files)
        except Exception:
            stats[key] = 0

    return stats


if __name__ == "__main__":
    # Quick test of the utilities
    print("Synthea Dataset Statistics:")
    print("=" * 40)

    if is_synthea_available():
        stats = get_test_data_stats()
        for key, count in stats.items():
            print(f"{key.replace('_', ' ').title()}: {count:,}")

        print("\nSample Test Cases:")
        print("-" * 20)

        synthea = SyntheaTestData()

        # API test cases
        api_cases = synthea.create_api_test_cases(3)
        print(f"\nAPI Test Cases ({len(api_cases)}):")
        for case in api_cases:
            print(f"  - {case['description']}")

        # SFTP test cases
        sftp_cases = synthea.create_sftp_test_cases(2, 2)
        print(f"\nSFTP Test Cases ({len(sftp_cases)}):")
        for case in sftp_cases:
            print(f"  - {case['description']}")
    else:
        print("Synthea dataset not available at expected location.")
        print(f"Expected: {SYNTHEA_ROOT}")
