import csv
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from tqdm import tqdm

from pprint import pprint

with open('../config.yml', 'r') as f:
    config = yaml.load(f)

data_dir = Path("../data/")

# choose most recent crawl
folders = list(data_dir.glob("*_*"))
times = [datetime.strptime(folder.name, "%Y%m%d_%H%M%S") for folder in folders]
output_dir = folders[times.index(max(times))]

for query in config['queries'].keys():
    temp_dir = output_dir / query / "temp/"
    outfile = output_dir / query / "articles.csv"

    tempfiles = list(temp_dir.glob("*.json"))

    outcolumns = ["pmid", "doi", "title",
                  "journal", "pub_year", "pub_types",
                  "mesh_terms", "grants",
                  "authors", "author_affils", "error"]

    f = open(str(outfile), "w")
    csv_writer = csv.writer(f, delimiter=",")
    csv_writer.writerow(outcolumns)

    for file in tqdm(tempfiles):
        df = pd.read_json(file, orient="index")
        for ix, record in tqdm(df.iterrows(), leave=False):
            field_vals = {k: None for k in outcolumns}

            if pd.isna(record['MedlineCitation']):
                if "BookDocument" in record:
                    field_vals['error'] = "InvalidRecord: Book"
                    field_vals['pmid'] = record['BookDocument']['PMID']['text'][0]
                else:
                    field_vals['error'] = "InvalidRecord: Unknown"
            else:
                medline_data = record['MedlineCitation']['Article']
                pubmed_data = record['PubmedData']

                # PMID & DOI (IDs are stored in a weird way... pls don't touch this)
                for k, v in pubmed_data['ArticleIdList'].items():
                    if 'doi' in v['.attrs']:
                        field_vals['doi'] = v['text'][0]
                    if 'pubmed' in v['.attrs']:
                        field_vals['pmid'] = v['text'][0]
                
                # Title
                try:
                    field_vals['title'] = medline_data['ArticleTitle'][0]
                except:
                    try:
                        field_vals['title'] = medline_data['VernacularTitle'][0]
                    except:
                        pass

                # Journal title
                try:
                    field_vals['journal'] = medline_data['Journal']['Title'][0]
                except:
                    pass

                # Publication year
                try:
                    field_vals['pub_year'] = medline_data['ArticleDate']['Year'][0]
                except:
                    try:
                        field_vals['pub_year'] = medline_data['Journal']['JournalIssue']['PubDate']['Year'][0]
                    except:
                        try:
                            field_vals['pub_year'] = medline_data['Journal']['JournalIssue']['PubDate']['MedlineDate'][0][0:4]
                        except:
                            pass

                # Author Information
                authors = []
                author_affils = []
                if "AuthorList" in medline_data:
                    if "CollectiveName" in medline_data['AuthorList']['Author']:
                        authors = medline_data['AuthorList']['Author']['CollectiveName']
                        author_affils = []
                    else:
                        authorList = [v for k, v in medline_data['AuthorList'].items()
                                      if k != ".attrs"]
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

                field_vals['authors'] = authors
                field_vals['author_affils'] = author_affils

                # MeSH terms
                mesh_object = {}
                if 'MeshHeadingList' in record['MedlineCitation']:
                    meshitems = [v for k, v in record['MedlineCitation']['MeshHeadingList'].items()]
                    for mesh_item in meshitems:
                        desc = mesh_item['DescriptorName']['text'][0]

                        if len(mesh_item) == 1:
                            qual = np.nan
                        else:
                            qual = [v['text'][0]
                                    for k, v in mesh_item.items() if k != "DescriptorName"]
                        mesh_object[desc] = qual

                field_vals['mesh_object'] = mesh_object

                # Publicatoin
                try:
                    pub_types = [v['text'][0]
                                 for k, v in medline_data['PublicationTypeList'].items()]
                except:
                    pub_types = []
                field_vals['pub_types'] = pub_types

                # Grants
                try:
                    grants = [v['GrantID'][0]
                              for k, v in medline_data['GrantList'].items() if k != ".attrs"]
                except:
                    grants = []
                field_vals['grants'] = grants

            row = []
            for col in outcolumns:
                item = field_vals[col]

                if type(item) == list:
                    item = "; ".join(item)
                elif type(item) == dict:
                    item = json.dumps(item)
                elif pd.isna(item):
                    item = None
                else:
                    item = str(item)

                row.append(item)

            csv_writer.writerow(row)
    f.close()
