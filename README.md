# RediSearch Memory Footprint Comparisons

## Summary
Tool for comparing the Redis object and index memory footprints for various scenarios.

## Features
- Calculates memory footprint of Redis objects and search indices for various configurable scenarios
## Prerequisites
- Python
- Redis installation with Search + JSON modules
## Installation
1. Clone this repo.

2. Go to doc-memory-footprint folder.
```bash
cd doc-memory-footprint
```
3. Install Python requirements (either in a virtual env or global)
```bash
pip install -r requirements.txt
```
## Usage
### Options
- --url. Redis connection string.  Default = redis://localhost:6379
- --nkeys. Number of keys to be generated for each test.  Default = 10,000.
- --nfields. Number fields created for each key.  Default = 10.
- --textsize.  Number of random characters in each text or tag field.  Default = 10.
- --numericsize.  Number of random digits in each numeric field.  Default = 10.
- --format.  Output format for the table of consolidated results.  Options are text, html or markdown.  Default = text.
### Execution
```bash
python3 test.py --nkeys 200000 --format markdown
```
### Output
Sample output for the test above.

Consolidated Results - Num Keys:200000, Num Fields:10, Text Field Size:10, Numeric Field Size:10
| Index Structure       |   Object Size(b) |   Index Size(mb) |
|:----------------------|-----------------:|-----------------:|
| Hash Text Unsorted    |              269 |            35.45 |
| Hash Text Sorted      |              269 |           100.3  |
| Hash Tag              |              269 |            22.29 |
| Hash Numeric Unsorted |              241 |            33.12 |
| Hash Numeric Sorted   |              241 |            78.9  |
| JSON Text Unsorted    |              290 |            35.45 |
| JSON Text Sorted      |              290 |           100.3  |
| JSON Tag              |              290 |            22.29 |
| JSON Numeric Unsorted |              190 |            33.14 |
| JSON Numeric Sorted   |              190 |            78.91 |
