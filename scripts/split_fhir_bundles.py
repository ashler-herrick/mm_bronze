#!/usr/bin/env python3
"""
FHIR Bundle Splitter Utility

Safely extracts individual resources from large FHIR bundles to create
smaller test data files suitable for API ingestion testing.

Usage:
    python scripts/split_fhir_bundles.py --input /path/to/bundles --output /path/to/resources
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any
from collections import defaultdict


def extract_resources_from_bundle(bundle_path: Path) -> List[Dict[str, Any]]:
    """
    Extract individual resources from a FHIR bundle.

    Args:
        bundle_path: Path to the FHIR bundle JSON file

    Returns:
        List of individual FHIR resources
    """
    try:
        with open(bundle_path, "r", encoding="utf-8") as f:
            bundle = json.load(f)

        if bundle.get("resourceType") != "Bundle":
            print(f"Warning: {bundle_path} is not a FHIR Bundle (resourceType: {bundle.get('resourceType')})")
            return []

        resources = []
        entries = bundle.get("entry", [])

        print(f"Processing bundle: {bundle_path.name} ({len(entries)} entries)")

        for i, entry in enumerate(entries):
            resource = entry.get("resource")
            if resource and "resourceType" in resource:
                # Add metadata about the source bundle
                resource["_source"] = {
                    "bundleFile": bundle_path.name,
                    "bundleType": bundle.get("type"),
                    "entryIndex": i,
                    "extractedAt": "2025-06-06",  # Could use datetime.now()
                }
                resources.append(resource)
            else:
                print(f"  Warning: Entry {i} missing resource or resourceType")

        print(f"  Extracted {len(resources)} resources")
        return resources

    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {bundle_path}: {e}")
        return []
    except Exception as e:
        print(f"Error processing {bundle_path}: {e}")
        return []


def save_resources_by_type(resources: List[Dict[str, Any]], output_dir: Path, source_file: str):
    """
    Save resources organized by resource type.

    Args:
        resources: List of FHIR resources
        output_dir: Directory to save the extracted resources
        source_file: Original bundle filename for naming
    """
    # Group resources by type
    by_type = defaultdict(list)
    for resource in resources:
        resource_type = resource.get("resourceType", "Unknown")
        by_type[resource_type].append(resource)

    # Create output directories
    for resource_type in by_type.keys():
        type_dir = output_dir / resource_type.lower()
        type_dir.mkdir(parents=True, exist_ok=True)

    # Save resources
    saved_count = 0
    for resource_type, resource_list in by_type.items():
        type_dir = output_dir / resource_type.lower()

        for i, resource in enumerate(resource_list):
            # Create filename: resourcetype_bundlename_index.json
            base_name = Path(source_file).stem
            filename = f"{resource_type.lower()}_{base_name}_{i:03d}.json"
            resource_path = type_dir / filename

            try:
                with open(resource_path, "w", encoding="utf-8") as f:
                    json.dump(resource, f, indent=2, ensure_ascii=False)
                saved_count += 1
            except Exception as e:
                print(f"Error saving {resource_path}: {e}")

    print(f"  Saved {saved_count} individual resources")
    return saved_count


def analyze_bundle_sizes(bundle_dir: Path):
    """Analyze sizes of bundle files to identify candidates for splitting."""
    print("\n=== Bundle Size Analysis ===")
    bundle_files = list(bundle_dir.glob("*.json"))

    if not bundle_files:
        print(f"No JSON files found in {bundle_dir}")
        return []

    sizes = []
    for bundle_file in bundle_files:
        size_mb = bundle_file.stat().st_size / (1024 * 1024)
        sizes.append((bundle_file, size_mb))

    # Sort by size (largest first)
    sizes.sort(key=lambda x: x[1], reverse=True)

    print(f"Found {len(bundle_files)} bundle files:")
    large_bundles = []

    for bundle_file, size_mb in sizes:
        status = "LARGE" if size_mb > 5 else "OK"
        print(f"  {bundle_file.name}: {size_mb:.1f} MB [{status}]")

        if size_mb > 5:  # Bundles larger than 5MB
            large_bundles.append(bundle_file)

    print(f"\nFound {len(large_bundles)} bundles > 5MB that should be split")
    return large_bundles


def main():
    parser = argparse.ArgumentParser(description="Split FHIR bundles into individual resources")
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        required=True,
        help="Directory containing FHIR bundle files",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        required=True,
        help="Directory to save extracted resources",
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Only analyze bundle sizes, don't extract",
    )
    parser.add_argument(
        "--max-bundles",
        type=int,
        default=10,
        help="Maximum number of large bundles to process",
    )

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input directory {args.input} does not exist")
        sys.exit(1)

    # Analyze bundle sizes
    large_bundles = analyze_bundle_sizes(args.input)

    if args.analyze_only:
        return

    if not large_bundles:
        print("No large bundles found to split")
        return

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)

    # Process bundles
    print("\n=== Processing Large Bundles ===")
    total_resources = 0

    for i, bundle_path in enumerate(large_bundles[: args.max_bundles]):
        print(f"\n[{i + 1}/{min(len(large_bundles), args.max_bundles)}] Processing {bundle_path.name}")

        # Extract resources
        resources = extract_resources_from_bundle(bundle_path)
        if not resources:
            continue

        # Save resources
        saved = save_resources_by_type(resources, args.output, bundle_path.name)
        total_resources += saved

    print("\n=== Summary ===")
    print(f"Processed {min(len(large_bundles), args.max_bundles)} large bundles")
    print(f"Extracted {total_resources} individual resources")
    print(f"Resources saved to: {args.output}")

    # Show what was created
    print("\n=== Created Resource Types ===")
    for resource_dir in args.output.iterdir():
        if resource_dir.is_dir():
            file_count = len(list(resource_dir.glob("*.json")))
            print(f"  {resource_dir.name}: {file_count} files")


if __name__ == "__main__":
    main()
