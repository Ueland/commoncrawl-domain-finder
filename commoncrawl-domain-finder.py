
# Start app config 

# URL to index file, can be found at https://index.commoncrawl.org/
index_file = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/cc-index.paths.gz"

# End app config

import requests
import gzip
import json
import os
import shutil
import urllib.parse
import urllib.request

index_url_parsed = urllib.parse.urlparse(index_file)
url_prefix = index_url_parsed.scheme+"://"+index_url_parsed.netloc+"/"
http_headers = {'User-Agent': 'Commoncrawl domain names locator',}

if not os.path.exists("./cache"):
    os.mkdir("./cache")

if not os.path.exists("./domains"):
    os.mkdir("./domains")

print("Fetching index file")
with requests.get(index_file,stream=True, headers=http_headers) as res:
    extracted_index = gzip.decompress(res.content)
    for line in extracted_index.split(b'\n'):
        line = line.decode()

        # Only analyze files ending with .gz, the others are metadata we dont care about now
        if line.endswith(".gz"):

            print("\t- Downloading "+url_prefix+line)
            cdxres = requests.get(url_prefix+line, headers=http_headers, stream=True)
            with open("./cache/cdx-file.tmp.gz", "wb") as cdxout:
                shutil.copyfileobj(cdxres.raw, cdxout)
            #urllib.request.urlretrieve(url_prefix+line, "./cache/cdx-file.tmp.gz")

            print("\t- Unzipping file")
            with gzip.open('./cache/cdx-file.tmp.gz', 'r') as f_in, open('./cache/cdx-file.tmp', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove("./cache/cdx-file.tmp.gz")

            print("\t- Extracting URLs")
            with open("./cache/cdx-file.tmp", "r") as tf:
                for line in tf:
                    try:
                        bits = line.split("{", 1)
                        json_data = json.loads("{"+bits[1])
                        url = json_data['url']
                        with open("./cache/urls.txt", "a") as uf:
                            uf.write(url+"\n")
                    except json.decoder.JSONDecodeError:
                        continue
                        #print("\t\t- Failed parsing line, skipping, see debug.txt")

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
                    domain_path = "domains/"+domain[0].lower()+".txt"
                    if not os.path.exists(domain_path):
                        open(domain_path, "a+")

                    with open("domains/"+domain[0].lower()+".txt", "r") as df:
                        is_duplicate = False
                        for line in df:
                            if line == domain:
                                is_duplicate = True
                                break
                        if is_duplicate == False:
                            with open("domains/"+domain[0].lower()+".txt", "a") as dfw:
                                dfw.write(domain)

            print("\t- Sorting domain files")
            for path in os.listdir("./domains"):
                full_path = os.path.join("./domains", path)
                if os.path.isfile(full_path):
                    domainfile_r = open(full_path, "r")
                    sorted_domains = sorted(list(map(str, domainfile_r.readlines())))
                    domainfile_w = open(full_path, "w")
                    for sd in sorted_domains:
                        domainfile_w.write(sd)
                        

            os.remove("./cache/urls.txt")
            os.remove("./cache/cdx-file.tmp")
