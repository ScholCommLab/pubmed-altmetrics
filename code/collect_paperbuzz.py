import csv
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from helpers import load_config, select_basedir, PaperbuzzAPI

# Init logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Init
logging.info('Initialise configuration')
data_dir = Path("../data/")
config = load_config("../config.yml")

output_dir = select_basedir(data_dir)

# Initialize Paperbuzz API
paperbuzz = PaperbuzzAPI(config['contact']['email'])

# Output columns for results
out_columns = ["pmid", "doi", "resp", "err", "ts"]

# iterate over each query in config and collect altmetrics
logging.info('Query Paperbuzz for each query')
for query in config['queries'].keys():
    # Load input file
    input_df = pd.read_csv(output_dir / query / "articles.csv")
    input_df = input_df[input_df.error.isna()]

    f = open(str(output_dir / query / "paperbuzz.csv"), "a")
    csv_writer = csv.writer(f)
    csv_writer.writerow(out_columns)

    for pmid, doi in tqdm(zip(input_df.pmid, input_df.doi),
                          total=len(input_df),
                          desc="{}".format(query),
                          position=None,
                          bar_format="  {desc:<20}{percentage:3.0f}%|{bar}{r_bar}"):
        # Paperbuzz only provides metrics for articles with DOIs
        if not doi:
            continue

        now = datetime.now()

        try:
            resp = paperbuzz.search(doi)
            err = None
        except Exception as e:
            resp = None
            err = e

        row = [pmid, doi, json.dumps(resp), str(err), str(now)]
        csv_writer.writerow(row)
    f.close()
