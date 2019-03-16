import csv
import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from helpers import load_config, select_basedir

# Init logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Init
logging.info('Initialise configuration')
config = load_config("../config.yml")
data_dir = Path("../data/")
base_dir = select_basedir(data_dir)

# Create new dirs
results_dir = base_dir / "results"
if not os.path.exists(str(results_dir)):
    os.makedirs(str(results_dir))

# Metrics and fields to be exported
out_columns = ["pmid", "doi", "pub_year", "altmetric_id",
               "altmetric_queried", "paperbuzz_queried"]

altmetric_fields = config['altmetric_fields']
paperbuzz_fields = config['paperbuzz_fields']

am_metrics = sorted(altmetric_fields.keys())
paperbuzz_metrics = sorted(paperbuzz_fields.keys())

out_columns = out_columns + am_metrics + paperbuzz_metrics

logging.info('Processing article metrics ...')
for query in config['queries'].keys():
    articles = pd.read_csv(base_dir / query / "articles.csv", na_values="None")
    am_input = pd.read_csv(base_dir / query / "altmetric.csv", na_values="None")
    ced_input = pd.read_csv(base_dir / query / "paperbuzz.csv", na_values="None")

    f = open(str(base_dir / query / "metrics.csv"), "w")
    csv_writer = csv.writer(f)
    csv_writer.writerow(out_columns)

    rows = list(articles.iterrows())
    for ix, article in tqdm(rows,
                            desc="{}".format(query),
                            position=None,
                            bar_format="  {desc:<20}{percentage:3.0f}%|{bar}{r_bar}"):
        field_vals = {k: None for k in out_columns}

        # Add basic fields
        field_vals['pmid'] = article['pmid']
        field_vals['doi'] = article['doi']
        field_vals['pub_year'] = article['pub_year']

        # Parse altmetric results (if available)
        am_row = am_input[am_input['pmid'] == article['pmid']]
        if len(am_row) == 0:
            pass
        else:
            am_row = am_row.iloc[0]
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
        ced_row = ced_input[ced_input['doi'] == article['doi']]
        if len(ced_row) == 0:
            pass
        else:
            ced_row = ced_row.iloc[0]
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
        for c in out_columns:
            outrow.append(field_vals[c])

        csv_writer.writerow(outrow)
    f.close()

# ===========
# Process queries and subqueries
logging.info('Merging files and writing results ...')
normal_queries = []
subqueries = {}

for query, v in config['pubmed_queries'].items():
    if type(v) == list:
        subqueries[query] = [query+str(i+1) for i in range(len(v))]
    else:
        normal_queries.append(query)

# Queries to be merged
for merge, queries in subqueries.items():
    # Merge Metadata
    articles = []
    metrics = []

    for query in queries:
        articles.append(pd.read_csv(base_dir / query / "articles.csv"))
        metrics.append(pd.read_csv(base_dir / query / "metrics.csv"))

    articles = pd.concat(articles)
    articles = articles.drop_duplicates(subset="pmid")
    articles.to_csv(results_dir / "{}_metadata.csv".format(merge), index=False)

    metrics = pd.concat(metrics)
    metrics = metrics.drop_duplicates(subset="pmid")
    metrics.to_csv(results_dir / "{}_metrics.csv".format(merge), index=False)


for query in normal_queries:
    shutil.copy(str(base_dir / query / "articles.csv"),
                str(results_dir / "{}_metadata.csv".format(query)))
    shutil.copy(str(base_dir / query / "metrics.csv"),
                str(results_dir / "{}_metrics.csv".format(query)))
