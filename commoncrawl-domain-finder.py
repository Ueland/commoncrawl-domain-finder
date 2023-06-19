
# Start app config 

# URL to index file, can be found at https://index.commoncrawl.org/
index_file = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/cc-index.paths.gz"

# End app config

import os
import time
import gzip
import json
import shutil
import requests
import urllib.parse
import urllib.request

index_url_parsed = urllib.parse.urlparse(index_file)
url_prefix = index_url_parsed.scheme+"://"+index_url_parsed.netloc+"/"
http_headers = {'User-Agent': 'Commoncrawl domain names locator - https://github.com/Ueland/commoncrawl-domain-finder',}

if not os.path.exists("./cache"):
    os.mkdir("./cache")

if not os.path.exists("./domains"):
    os.mkdir("./domains")


completed_downloads = []
if not os.path.exists("./cache/downloaded.txt"):
    open("./cache/downloaded.txt", "a+")
with open("./cache/downloaded.txt", "r+") as df:
    completed_downloads = df.readlines()

print("Fetching index file")
with requests.get(index_file,stream=True, headers=http_headers) as res:
    extracted_index = gzip.decompress(res.content)
    for line in extracted_index.split(b'\n'):
        line = line.decode()
        wait_time = 10

        # Only analyze files ending with .gz, the others are metadata we dont care about now
        if line.endswith(".gz"):

            data_file_gz = url_prefix+line

            if data_file_gz+"\n" in completed_downloads:
                print("\t- Already downloaded "+url_prefix+line+", skipping")
                continue

            print("\t- Downloading "+data_file_gz)
            attempt_download = True

            while attempt_download:
                attempt_download = False
                cdxres = requests.get(data_file_gz, headers=http_headers, stream=True)
                if cdxres.headers["X-Cache"] == "Error from cloudfront":
                    print("\t\t- Downlading failed, retrying in "+str(wait_time)+"s")
                    time.sleep(wait_time)
                    wait_time += 10
                    attempt_download = True


            with open("./cache/cdx-file.tmp.gz", "wb") as cdxout:
                shutil.copyfileobj(cdxres.raw, cdxout)

            print("\t- Unzipping file")
            with gzip.open('./cache/cdx-file.tmp.gz', 'r') as f_in, open('./cache/cdx-file.tmp', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove("./cache/cdx-file.tmp.gz")

            print("\t- Extracting URLs")
            uf = open("./cache/urls.txt", "a")
            with open("./cache/cdx-file.tmp", "r") as tf:
                for line in tf:
                    try:
                        bits = line.split("{", 1)
                        json_data = json.loads("{"+bits[1])
                        url = json_data['url']
                        uf.write(url+"\n")
                    except json.decoder.JSONDecodeError:
                        continue
            uf.close()

            print("\t- Extracting domain names from URLs")
            domains_to_save = set()
            with open("./cache/urls.txt", "r") as uf:
                for line in uf:
                    parsed = urllib.parse.urlparse(line)
                    domain = parsed.netloc

                    if ":" in domain:
                        domain = domain.split(":")[0]
                    domain = domain+"\n"
                    
                    if domain not in domains_to_save:
                        domains_to_save.add(domain)

            print("\t- Saving "+str(len(domains_to_save))+" domains to disk")
            for domain in domains_to_save:
                domain = domain.strip().lower()
                domain_path = "domains/"+domain[0]+".txt"
                if not os.path.exists(domain_path):
                    open(domain_path, "a+")

                with open("domains/"+domain[0].lower()+".txt", "a") as dfw:
                    dfw.write(domain)

            os.remove("./cache/urls.txt")
            os.remove("./cache/cdx-file.tmp")
            with open("./cache/downloaded.txt", "a") as dl:
                dl.write(data_file_gz+"\n")

print("Everything downloaded")

print("Removing duplicates and sorting domain lists")
for path in os.listdir("./domains"):
    full_path = os.path.join("./domains", path)
    domains = set()
    domaincounter = 0
    with open(full_path, "r") as uf:
        for domain in uf:
            domaincounter +=1
            if domain not in domains:
                domains.add(domain)
    domainsfile_w = open(full_path, "w")
    for sd in sorted(domains):
        domainsfile_w.write(sd)
    domainsfile_w.close()
    print("\t- "+path+" had "+ str(domaincounter)+" domains, now has "+str(len(domains)))

print("Cleaning up")
os.remove("./cache/downloaded.txt")
os.rmdir("./cache")

print("All done")
