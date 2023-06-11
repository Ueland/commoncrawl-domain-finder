# About
Python script for finding all unique domain names available in the Common Crawl dataset (https://commoncrawl.org/).

# Installing/running
Clone repo locally
Install requirements: `pip3 install -r requirements.txt`
Run it: `python3 commoncrawl-domain-finder.py`

# Output
This script will create the following content on your machine:
- A cache folder, used as work-space, will get cleaned by the app
- A domains folder, each domain found is stored in /domains/[first letter of domain].txt

# A note on performance
This first version is hacked together in a few hours and misses multi-core support.
So it currently only downloads a single file and processes it on one core, before it cleans up and continues to the next file.
It will typically need to download 200 files totalling around 200GB gzipped, if you want the complete dataset.
