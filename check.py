"""Scholix / DataCite Link Coverage Verification Script.

This script supports the analysis described in the project README. It takes two
input CSV files containing pairs of Dataset PIDs and Literature PIDs that were
reported as missing in ScholExplorer and/or DataCite, queries the ScholExplorer
API, and produces enriched CSV reports plus summary coverage statistics.

Input CSV files (semicolon separated; header in first line):
1. 20251124_no_Scholix_but_DataCite.csv
     Links alleged to exist in DataCite but not in ScholExplorer.
     Columns used:
         - Dataset_PID
         - Literature_PID
         - DataCite_RelationType_of_Dataset_PID_record (relation type expected)
2. 20251124_no_Scholix_no_DataCite.csv
     Links alleged to be missing in both DataCite and ScholExplorer.
     Columns used:
         - Dataset_PID
         - Literature_PID

Generated output reports:
1. no_scholix_but_datacite_check.csv
2. no_scholix_no_datacite.csv

Each output row contains original identifiers plus:
    - links_in_scholix: integer total returned by ScholExplorer for the query
    - url_to_scholix : fully constructed API URL for reproducibility

ScholExplorer API endpoint pattern used:
    GET /v3/Links?sourcePid=<Dataset_PID>&targetPid=<Literature_PID>&relation=<RELATION?>
The relation query parameter is only included for the first dataset when the
DataCite relation type column is present.

Usage (from project root, after installing dependencies):
    python check.py

Assumptions / Notes:
* The CSV delimiter is a semicolon (';').
* The script streams rows; for summary it re-reads generated output files.
* Network errors are not explicitly handled; any request failure will raise via
    requests. For robustness you could wrap calls in try/except and record errors.
* The ScholExplorer API response is assumed to contain a JSON object with key
    'totalLinks'. If the schema changes, a KeyError will be raised.

License / Attribution: Documentation added based on analytical results reported
in README. Functional logic unchanged.
"""

import csv
import requests
import json


scholexplorer_domain = "https://api.scholexplorer.openaire.eu/"  # Base API URL

def read_csv(file_path):
    """Yield rows from a semicolon-delimited CSV as dictionaries.

    The function manually reads the header line to strip potential BOM markers
    and then feeds the remaining lines to ``csv.DictReader`` with the normalized
    header.

    Parameters
    ----------
    file_path : str
        Path to the CSV file.

    Yields
    ------
    dict
        A mapping of column name to string value for each row.

    Notes
    -----
    This is a generator; it does not load the entire file into memory.
    """
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        # explicitly read the first line as header (handles BOM) and use it as fieldnames
        header = next(csvfile).strip().split(';')
        header = [h.lstrip('\ufeff').replace('"','') for h in header]
        reader = csv.DictReader(csvfile, fieldnames=header, delimiter=';',quotechar='"')
        for row in reader:
            yield row
 
 
def check_stats_no_scholix_but_datacite(): 
    """Process rows alleged present in DataCite but missing in ScholExplorer.

    Reads ``20251124_no_Scholix_but_DataCite.csv`` and queries the ScholExplorer
    API including the relation type for each pair. Builds a list of statistics
    and writes them to ``no_scholix_but_datacite_check.csv``.

    Output columns:
        Dataset_PID, Literature_PID, DataCite_RelationType_of_Dataset_PID_record,
        links_in_scholix, url_to_scholix

    Returns
    -------
    None
        Side-effect: writes a CSV file and prints progress/stat rows.
    """
    stats = []           
    for item in read_csv("20251124_no_Scholix_but_DataCite.csv"):
        print("requesting ", item['Dataset_PID'])
        source_pid= item['Dataset_PID']
        target_pid= item['Literature_PID']
        relation = item['DataCite_RelationType_of_Dataset_PID_record']
        method_path = f"/v3/Links?sourcePid={source_pid}&targetPid={target_pid}&relation={relation}"
        scholix_url= f"{scholexplorer_domain}{method_path}"
        links_in_scholix = requests.get(scholix_url).json()['totalLinks']
        current_stats = {
            "Dataset_PID": item['Dataset_PID'],
            "Literature_PID": item['Literature_PID'],
            "DataCite_RelationType_of_Dataset_PID_record": item['DataCite_RelationType_of_Dataset_PID_record'],
            "links_in_scholix": links_in_scholix,
            "url_to_scholix": scholix_url,
        }
        stats.append(current_stats)
        print(json.dumps(current_stats, indent=2))
        
    if not stats:
        print("No stats to write.")
    else:
        out_path = "no_scholix_but_datacite_check.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(stats[0].keys()), delimiter=';')
            writer.writeheader()
            for row in stats:
                writer.writerow(row)
        print(f"Wrote {len(stats)} rows to {out_path}")

def check_stats_no_scholix_no_datacite():
    """Process rows alleged missing in both DataCite and ScholExplorer.

    Reads ``20251124_no_Scholix_no_DataCite.csv`` and queries the ScholExplorer
    API without a relation filter. Writes enriched results to
    ``no_scholix_no_datacite.csv``.

    Output columns:
        Dataset_PID, Literature_PID, links_in_scholix, url_to_scholix

    Returns
    -------
    None
        Side-effect: writes a CSV file and prints progress/stat rows.
    """
    stats=[]    
    for item in read_csv("20251124_no_Scholix_no_DataCite.csv"):
        print("requesting ", item['Dataset_PID'])
        source_pid= item['Dataset_PID']
        target_pid= item['Literature_PID']
        method_path = f"/v3/Links?sourcePid={source_pid}&targetPid={target_pid}"
        scholix_url= f"{scholexplorer_domain}{method_path}"
        links_in_scholix = requests.get(scholix_url).json()['totalLinks']
        current_stats = {
            "Dataset_PID": item['Dataset_PID'],
            "Literature_PID": item['Literature_PID'],
            "links_in_scholix": links_in_scholix,
            "url_to_scholix": scholix_url,
        }
        stats.append(current_stats)
        print(json.dumps(current_stats, indent=2))
        
    if not stats:
        print("No stats to write.")
    else:
        out_path = "no_scholix_no_datacite.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(stats[0].keys()), delimiter=';')
            writer.writeheader()
            for row in stats:
                writer.writerow(row)
        print(f"Wrote {len(stats)} rows to {out_path}")

if __name__ == "__main__":
    check_stats_no_scholix_but_datacite()
    check_stats_no_scholix_no_datacite()

    total_rows = 0

    no_scholix_but_datacite_stats =[k for k in read_csv("no_scholix_but_datacite_check.csv")]
    no_scholix_no_datacite_stats =[k for k in read_csv("no_scholix_no_datacite.csv")]

    scholix_that_have_links =len([k for k in no_scholix_but_datacite_stats if int(k['links_in_scholix']) > 0])

    print("No Scholix but DataCite After Check")
    print("total rows:", len(no_scholix_but_datacite_stats))
    print("rows with links in Scholix:", scholix_that_have_links)
    print("percentage:", (scholix_that_have_links/len(no_scholix_but_datacite_stats))*100 if len(no_scholix_but_datacite_stats) >0 else 0)


    print("No Scholix no DataCite:", len(no_scholix_no_datacite_stats))
    print("total rows:", len(no_scholix_no_datacite_stats))
    scholix_that_have_links =len([k for k in no_scholix_no_datacite_stats if int(k['links_in_scholix']) > 0])
    print("rows with links in Scholix but not in datacite:", scholix_that_have_links)
    print("percentage of links in Scholix but not in Datacite", scholix_that_have_links/len(no_scholix_no_datacite_stats)*100 if len(no_scholix_no_datacite_stats) >0 else 0)