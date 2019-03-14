# !/usr/local/bin/python
# -*- coding: utf-8 -*-
import datetime

import requests
import yaml


class PaperbuzzAPI(object):
    def __init__(self, email, version="v0"):
        self.endpoint = "https://api.paperbuzz.org/" + version + "/doi/"
        self.params = {
            'email': email
        }

    def search(self, doi):
        params = self.params
        headers = {}

        try:
            req = requests.get(
                self.endpoint + doi,
                params=params,
                headers=headers)
            if req.status_code == 200:
                return req.json()
            else:
                return None
        except Exception as e:
            raise


def load_config(path):
    with open('../config.yml', 'r') as f:
        config = yaml.load(f)

        config['queries'] = {}
        for query, v in config['pubmed_queries'].items():
            if type(v) == list:
                for i, s in enumerate(v, start=1):
                    config['queries'][query+str(i)] = s
            else:
                config['queries'][query] = v

        return config


def select_basedir(data_dir):
    # choose most recent crawl
    folders = list(data_dir.glob("*_*"))
    times = [datetime.datetime.strptime(folder.name, "%Y%m%d_%H%M%S") for folder in folders]
    return folders[times.index(max(times))]
