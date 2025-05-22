from typing import Dict, List, Any

from pathlib import Path

repo_root = Path(__file__).parent.parent

#so we can add HL7 or something else later
data_root = repo_root / "test_data"
fhir_root = data_root / "fhir"


#Examples: 
# /ingest/json/fhir/r6/patient
# /ingest/xml/fhir/r6/patient
# /ingest/text/hl7/v2/lab
#General:
#    "/{fmt}/{content_type}/{version}/{subtype}",

def load_fhir_test_data() -> List[Dict[str, Any]]:
    #provide a path <> ingest_url mapping for the test function
    # Example path: test_data/fhir/r6/patient.json
    # 2. Get the version e.g. r6
    # 3. Get the resource type e.g. patient
    # 4. Get the extension
    # 5. Build the ingest url, supply the path
    all_paths = sorted(data_root.rglob(f"*/*/*"))
    data = []
    for path in all_paths:
        if not path.is_file():
            continue

        print(path)
        parts = path.parts
        subtype = parts[-2]
        version = parts[-3]
        fmt = parts[-4]
        ext = parts[-1].split('.')[-1]
        entry = {
            "path" : path,
            "url" : f"http://localhost:8000/ingest/{ext}/{fmt}/{version}/{subtype}",
            "ext" : ext
        }
        data.append(entry)

    return data


if __name__ == "__main__":
    data = load_fhir_test_data()
    print(data)