import csv
import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from altmetric import Altmetric
from tqdm import tqdm

from helpers import PaperbuzzAPI, load_config, select_basedir

# Init
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logging.info('Initialise configuration')

data_dir = Path("../data/")
config = load_config("../config.yml")
base_dir = select_basedir(data_dir)
results_dir = base_dir / "results"

if not os.path.exists(str(results_dir)):
    os.makedirs(str(results_dir))

# Output columns for results
OUT_COLUMNS = ["pmid", "doi", "resp", "err", "ts"]
metrics_columns = ["pmid", "doi", "pub_year", "altmetric_id",
                   "altmetric_queried", "paperbuzz_queried"]

altmetric_fields = config['altmetric_fields']
paperbuzz_fields = config['paperbuzz_fields']

am_metrics = sorted(altmetric_fields.keys())
paperbuzz_metrics = sorted(paperbuzz_fields.keys())

metrics_columns = metrics_columns + am_metrics + paperbuzz_metrics


# Initialize APIs
ALTMETRIC_KEY = config['keys']['altmetric']
altmetric = Altmetric(ALTMETRIC_KEY)
paperbuzz = PaperbuzzAPI(config['contact']['email'])

logging.info('Iterating over input files')
for name, input_file in config['input_files'].items():
    logging.info('Processing {}: {}'.format(name, input_file))

    # Load input file
    input_df = pd.read_csv(data_dir / input_file)

    output_dir = base_dir / name
    if not os.path.exists(str(output_dir)):
        os.makedirs(str(output_dir))

    # # Query Paperbuzz API
    # logging.info('Querying Paperbuzz API')
    # with open(str(output_dir / "paperbuzz.csv"), "w") as f:
    #     csv_writer = csv.writer(f)
    #     csv_writer.writerow(OUT_COLUMNS)

    #     for doi in tqdm(input_df.doi, total=len(input_df)):
    #         now = datetime.now()

    #         try:
    #             resp = paperbuzz.search(doi)
    #             err = None
    #         except Exception as e:
    #             resp = None
    #             err = e

    #         row = [None, doi, json.dumps(resp), str(err), str(now)]
    #         csv_writer.writerow(row)

    # # Query Altmetric API
    # logging.info('Querying Altmetric API')
    # with open(str(output_dir / "altmetric.csv"), "w") as f:
    #     csv_writer = csv.writer(f)
    #     csv_writer.writerow(OUT_COLUMNS)

    #     for doi in tqdm(input_df.doi, total=len(input_df)):
    #         now = datetime.now()

    #         try:
    #             resp = altmetric.doi(doi)
    #             err = None
    #         except Exception as e:
    #             resp = None
    #             err = e

    #         row = [None, doi, json.dumps(resp), str(err), str(now)]
    #         csv_writer.writerow(row)

    #########################
    # Process metrics
    #########################
    logging.info('Processing article metrics ...')
    am_input = pd.read_csv(output_dir / "altmetric.csv", na_values="None")
    ced_input = pd.read_csv(output_dir / "paperbuzz.csv", na_values="None")

    f = open(str(output_dir / "metrics.csv"), "w")
    csv_writer = csv.writer(f)
    csv_writer.writerow(metrics_columns)

    rows = list(input_df.iterrows())
    for ix, article in tqdm(rows):
        field_vals = {k: None for k in metrics_columns}
        if "pmid" in article:
            field_vals['pmid'] = article['pmid']
        else:
            field_vals['pmid'] = None

        field_vals['doi'] = article['doi']
        field_vals['pub_year'] = article['pub_year']

        # Parse altmetric results (if available)
        am_row = am_input[am_input.doi == article['doi']]
        if len(am_row) == 0:
            am_row = am_row.iloc[0]
        else:
            if pd.isna(am_row['resp']):
                pass
            else:
                response = json.loads(am_row['resp'])

                field_vals['altmetric_id'] = response['altmetric_id']
                field_vals['altmetric_queried'] = am_row['ts']

                for metric in am_metrics:
                    try:
                        field_vals[metric] = response[altmetric_fields[metric]]
                    except Exception as e:
                        pass

        # Parse Paperbuzz results (if available)
        ced_row = ced_input[ced_input.doi == article['doi']]
        if len(ced_row) == 0:
            ced_row = ced_row.iloc[0]
        else:
            if pd.isna(ced_row['resp']):
                pass
            else:
                response = json.loads(str(ced_row['resp']))

                field_vals['paperbuzz_queried'] = ced_row['ts']

                counts = {v: None for k, v in paperbuzz_fields.items()}

                for source in response['altmetrics_sources']:
                    if source['source_id'] in paperbuzz_fields.values():
                        counts[source['source_id']] = source['events_count']

                for m in paperbuzz_metrics:
                    field_vals[m] = counts[paperbuzz_fields[m]]

        outrow = []
        for c in metrics_columns:
            outrow.append(field_vals[c])

        csv_writer.writerow(outrow)
    f.close()

    logging.info('Writing results')

    shutil.copy(str(data_dir / input_file),
                str(results_dir / "{}_metadata.csv".format(name)))
    shutil.copy(str(output_dir / "metrics.csv"),
                str(results_dir / "{}_metrics.csv".format(name)))
