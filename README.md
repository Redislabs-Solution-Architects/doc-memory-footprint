# RediSearch Memory Footprint Comparisons

## Blog
https://joeywhelan.blogspot.com/2023/02/redis-hash-vs-json-memory-footprints.html

## Summary
Tool for comparing the Redis object and index memory footprints for various scenarios.

## Architecture
![architecture](arch.jpg)
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
python3 mptest.py --nkeys 1000000 --nfields 15 --textsize 10 --numericsize 10 --format markdown
```
### Output
Sample output for the test above.

Consolidated Results - Num Keys:1000000, Num Fields:15, Text Field Size:10, Numeric Field Size:10
| Index Structure       |   Object Size(b) |   Index Size(mb) |
|:----------------------|-----------------:|-----------------:|
| Hash Text Unsorted    |              269 |           199.15 |
| Hash Text Sorted      |              269 |           637.84 |
| Hash Tag Unsorted     |              269 |           133.34 |
| Hash Tag Sorted       |              269 |           572.03 |
| Hash Numeric Unsorted |              245 |           192.3  |
| Hash Numeric Sorted   |              245 |           535.62 |
| JSON Text Unsorted    |              290 |           199.13 |
| JSON Text Sorted      |              290 |           637.82 |
| JSON Tag Unsorted     |              290 |           133.32 |
| JSON Tag Sorted       |              290 |           572.02 |
| JSON Numeric Unsorted |              190 |           192.3  |
| JSON Numeric Sorted   |              190 |           535.62 |
