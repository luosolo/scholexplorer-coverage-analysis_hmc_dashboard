# Scholix DataCite Link Coverage Analysis HMC Dashboard

## Overview

This project originated from a ticket opened for ScholExplorer reporting missing links that were allegedly present in DataCite but absent in ScholExplorer.

## Purpose

The script analyzes two CSV files to verify link coverage discrepancies:

1. **First CSV** (`20251124_no_Scholix_but_DataCite.csv`): Contains links present in DataCite but presumed to be missing in ScholExplorer
2. **Second CSV** (`20251124_no_Scholix_no_DataCite.csv`): Contains links absent in both DataCite and ScholExplorer

## Usage

### Setup

1. Create a virtual environment:
```bash
python3 -m venv .venv
```

2. Activate the virtual environment:
```bash
source .venv/bin/activate
```

3. Install required dependencies:
```bash
pip install requests
```

### Running the Script

Execute the analysis script:
```bash
python check.py
```

The script will process both CSV files and generate detailed reports with query methods for each link.

## Results

The analysis demonstrates:

- **First CSV**: ScholExplorer achieves **98% coverage** of links claimed to be missing, contradicting the initial report
- **Second CSV**: Approximately **68% of links** not present in DataCite are actually available in ScholExplorer

## Output

The script generates two detailed reports:

- Each report includes all analyzed links
- For each link, the report specifies the correct HTTP method (GET/POST) to use when querying ScholExplorer
- This allows for easy verification and reproduction of the coverage analysis

## Conclusion

These findings show that ScholExplorer maintains excellent coverage of scholarly links, often exceeding DataCite's index.