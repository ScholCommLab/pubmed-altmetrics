import csv
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from tqdm import tqdm

with open('../config.yml', 'r') as f:
    config = yaml.load(f)

data_dir = Path("../data/")

# choose most recent crawl
folders = list(data_dir.glob("*"))
times = [datetime.strptime(folder.name, "%Y%m%d_%H%M%S") for folder in folders]
output_dir = folders[times.index(max(times))]

for query in config['queries'].keys():
    temp_dir = output_dir / query / "temp/"
    outfile = output_dir / query / "articles.csv"

    tempfiles = list(temp_dir.glob("*.json"))

    outcolumns = ["pmid", "doi", "title",
                  "journal", "pub_year", "pub_types",
                  "mesh_terms", "grants",
                  "authors", "author_affils"]

    f = open(str(outfile), "w")
    csv_writer = csv.writer(f, delimiter=",")
    csv_writer.writerow(outcolumns)

    for file in tqdm(tempfiles):
        df = pd.read_json(file, orient="index")
        for ix, record in tqdm(df.iterrows(), leave=False):
            if pd.isna(record['MedlineCitation']):
                continue
            medline_data = record['MedlineCitation']['Article']
            pubmed_data = record['PubmedData']

            # Title
            try:
                title = medline_data['ArticleTitle'][0]
            except:
                try:
                    title = medline_data['VernacularTitle'][0]
                except:
                    title = None

            # Journal title
            try:
                journal = medline_data['Journal']['Title'][0]
            except:
                journal = None

            # Publication year
            try:
                pub_year = medline_data['ArticleDate']['Year'][0]
            except:
                try:
                    pub_year = medline_data['Journal']['JournalIssue']['PubDate']['Year'][0]
                except:
                    try:
                        pub_year = medline_data['Journal']['JournalIssue']['PubDate']['MedlineDate'][0][0:4]
                    except:
                        pub_year = None

            # PMID & DOI (IDs are stored in a weird way... pls don't touch this)
            doi = None
            pmid = None
            for k, v in pubmed_data['ArticleIdList'].items():
                if 'doi' in v['.attrs']:
                    doi = v['text'][0]
                if 'pubmed' in v['.attrs']:
                    pmid = v['text'][0]

            # Author Information
            authors = []
            author_affils = []
            if "AuthorList" in medline_data:
                if "CollectiveName" in medline_data['AuthorList']['Author']:
                    authors = medline_data['AuthorList']['Author']['CollectiveName']
                    author_affils = []
                else:
                    authorList = [v for k, v in medline_data['AuthorList'].items() if k != ".attrs"]
                    for author in authorList:
                        if 'ForeName' in author:
                            forename = author['ForeName'][0]
                        else:
                            forename = np.nan
                        if 'LastName' in author:
                            lastname = author['LastName'][0]
                        else:
                            lastname = np.nan

                        try:
                            affil = author['AffiliationInfo']['Affiliation'][0]
                        except:
                            affil = np.nan

                        authors.append(", ".join([str(forename), str(lastname)]))
                        author_affils.append(str(affil))
            else:
                authors = []
                author_affils = []

            # MeSH terms
            mesh_object = {}
            if 'MeshHeadingList' in record['MedlineCitation']:
                meshitems = [v for k, v in record['MedlineCitation']['MeshHeadingList'].items()]
                for mesh_item in meshitems:
                    desc = mesh_item['DescriptorName']['text'][0]

                    if len(mesh_item) == 1:
                        qual = np.nan
                    else:
                        qual = [v['text'][0] for k, v in mesh_item.items() if k != "DescriptorName"]
                    mesh_object[desc] = qual

            # Publicatoin
            try:
                pub_types = [v['text'][0] for k, v in medline_data['PublicationTypeList'].items()]
            except:
                pub_types = []

            # Grants
            try:
                grants = [v['GrantID'][0]
                          for k, v in medline_data['GrantList'].items() if k != ".attrs"]
            except:
                grants = []

            row = [pmid,
                   doi,
                   title,
                   journal,
                   pub_year,
                   "; ".join(pub_types),
                   json.dumps(mesh_object),
                   "; ".join(grants),
                   "; ".join(authors),
                   "; ".join(author_affils)]

            csv_writer.writerow(row)
    f.close()
