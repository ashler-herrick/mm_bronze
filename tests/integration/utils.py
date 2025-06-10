from typing import Dict, List, Any

from pathlib import Path

repo_root = Path(__file__).parent.parent

# so we can add HL7 or something else later
data_root = repo_root / "test_data"
fhir_root = data_root / "fhir"


# Examples:
# /ingest/json/fhir/r6/patient
# /ingest/xml/fhir/r6/patient
# /ingest/text/hl7/v2/lab
# General:
#    "/{fmt}/{content_type}/{version}/{subtype}",


def load_fhir_test_data() -> List[Dict[str, Any]]:
    """
    Load FHIR test data, preferring smaller individual resources over large bundles.

    Returns list of test files with their corresponding ingestion URLs.
    """
    # Check for individual resources first (better for API testing with 10MB limit)
    resources_root = fhir_root / "resources"
    if resources_root.exists():
        print(f"Using individual resources from {resources_root}")
        return load_individual_resources(resources_root)

    # Fallback to bundle or other test data
    print(f"Using legacy test data from {fhir_root}")
    return load_legacy_test_data(fhir_root)


def load_individual_resources(resources_root: Path, max_files: int = 20) -> List[Dict[str, Any]]:
    """Load individual FHIR resources for API testing."""
    data = []

    # Sample a few files from each resource type for testing
    for resource_type_dir in sorted(resources_root.iterdir()):
        if not resource_type_dir.is_dir():
            continue

        resource_files = list(resource_type_dir.glob("*.json"))
        if not resource_files:
            continue

        # Take up to 2 files per resource type to keep test runtime reasonable
        sample_files = resource_files[:2]

        for path in sample_files:
            entry = {
                "path": path,
                "url": f"http://localhost:8000/ingest/json/fhir/r4/{resource_type_dir.name}",
                "ext": "json",
                "resource_type": resource_type_dir.name,
                "size_mb": path.stat().st_size / (1024 * 1024),
            }
            data.append(entry)

            if len(data) >= max_files:
                break

        if len(data) >= max_files:
            break

    print(f"Selected {len(data)} individual resource files for testing")
    return data


def load_legacy_test_data(fhir_root: Path) -> List[Dict[str, Any]]:
    """Load legacy test data structure."""
    all_paths = sorted(fhir_root.rglob("*/*/*"))
    data = []
    for path in all_paths:
        if not path.is_file():
            continue

        print(path)
        parts = path.parts
        subtype = parts[-2]
        version = parts[-3]
        fmt = parts[-4]
        ext = parts[-1].split(".")[-1]
        entry = {
            "path": path,
            "url": f"http://localhost:8000/ingest/{ext}/{fmt}/{version}/{subtype}",
            "ext": ext,
        }
        data.append(entry)

    return data


if __name__ == "__main__":
    data = load_fhir_test_data()
    print(data)
