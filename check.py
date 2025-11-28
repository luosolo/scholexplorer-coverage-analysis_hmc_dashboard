import csv
import requests
import json


scholexplorer_domain = "https://api.scholexplorer.openaire.eu/"

def read_csv(file_path):
    """Reads a CSV file and returns its content as a list of dictionaries."""
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        # explicitly read the first line as header (handles BOM) and use it as fieldnames
        header = next(csvfile).strip().split(';')
        header = [h.lstrip('\ufeff').replace('"','') for h in header]
        reader = csv.DictReader(csvfile, fieldnames=header, delimiter=';',quotechar='"')
        for row in reader:
            yield row
 
 
def check_stats_no_scholix_but_datacite(): 
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