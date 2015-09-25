#!/usr/bin/env python

import json
from elasticsearch import Elasticsearch


class Index(object):
    def __init__(self, index_name):
        self.es = Elasticsearch()
        self.index_name = index_name

    def create(self):
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
        self.es.indices.create(index=self.index_name, body={
            "settings": {
                "index": {
                    "analysis": {
                        "analyzer": {
                            "with_stemming": {
                                "type" : "custom",
                                "filter" : [
                                    "standard",
                                    "lowercase",
                                    "stemmer_english",
                                ],
                                "tokenizer" : "standard"
                            },
                        },
                        "filter" : {
                            "stemmer_english" : {
                                "type" : "stemmer",
                                "name" : "porter2"
                            },
                        }
                    }
                }
            },
            "mappings": {
                "logline": {
                    "properties": {
                        "text": {
                            "type": "string",
                            "fields": {
                                "stemmed": {
                                    "type": "string",
                                    "analyzer": "with_stemming",
                                }
                            }
                        }
                    }
                }
            }
        })

    def add(self, line):
        id = line.pop("_id")
        doc_type = line.pop("_type")
        self.es.index(
            index=self.index_name,
            doc_type=doc_type,
            id=id,
            body=line,
        )


def index(filename):
    index = Index("missions")
    index.create()
    for count, line in enumerate(open(filename)):
        index.add(json.loads(line))
        print count


if __name__ == "__main__":
    index("data/missions.jsonl")
