#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import whoosh.index as index
import whoosh.qparser as qparser
import whoosh.fields as fields
import glob
import fire

"""A simple full text search example Fire CLI.
Example usage:
pipenv run fts.py create_index
pipenv run fts.py search pattern.txt
"""


def create_index(index_dir_name="./index", log_file_name="./logs/*.log"):
    """Initialized the index from log file. 
    If the index directory does not exists, create it.
    If the index directory does exists, add the index.
    """
    ix = get_index(index_dir_name)
    writer = ix.writer()
    for abspath in list_abspath(log_file_name):
        with open(abspath, "r") as fp:
            for line in fp:
                writer.add_document(path=abspath, body=line)
    writer.commit()
    ix.close()


def search(text, index_dir_name="./index"):
    """Simple search."""
    ix = get_index(index_dir_name)
    searcher = ix.searcher()
    query = qparser.QueryParser("body", schema=ix.schema)
    for search_result in searcher.search(query.parse(text)):
        print(search_result["path"], search_result["body"], end="")
    ix.close()


def pattern(pattern_file_name, index_dir_name="./index"):
    """Search from pattern file."""
    ix = get_index(index_dir_name)
    searcher = ix.searcher()
    query = qparser.QueryParser("body", schema=ix.schema)
    with open(pattern_file_name, "r") as fp:
        for line in fp:
            for search_result in searcher.search(query.parse(line)):
                print(search_result["path"], search_result["body"], end="")
    ix.close()


def get_index(idx):
    if os.path.exists(idx):
        ix = index.open_dir(idx)
    else:
        schema = fields.Schema(
            path=fields.ID(stored=True),
            body=fields.NGRAM(stored=True)
        )
        os.mkdir(idx)
        ix = index.create_in(idx, schema)
    return ix


def list_abspath(path):
    return [os.path.abspath(p) for p in glob.glob(path)]


def main():
    fire.Fire(name='fts')


if __name__ == '__main__':
    main()
