#!/usr/bin/python3

import argparse
import os
import sys
from contextlib import contextmanager

import ezodf

import ods_columns as cols


# C O L L O C A T I O N S

@contextmanager
def ods_set(filepath):
    file_abspath = os.path.abspath(filepath)
    doc = ezodf.opendoc(file_abspath)
    m = make_collocations(doc.sheets[0])
    try:
        yield m
    finally:
        m.clear()


def make_collocations(sheet):
    collocations = set()
    for i, row in enumerate(read_row(sheet)):
        oewnsensekey1 = row[cols.sensekey1_col].value
        oewnsensekey2 = row[cols.sensekey2_col].value
        if oewnsensekey1 == oewnsensekey2:
            print(f'{i + 1} {oewnsensekey1} - {oewnsensekey1} reflexive', file=sys.stderr)
        collocation = frozenset({oewnsensekey1, oewnsensekey2})
        if collocation in collocations:
            print(f'{i + 1} {collocation} duplicate', file=sys.stderr)
        collocations.add(collocation)
    return collocations


def read_row(sheet):
    for row in range(sheet.nrows()):
        yield [sheet[row, col] for col in range(sheet.ncols())]


def run():
    parser = argparse.ArgumentParser(description="load from ODS, inspect well-formedness")
    parser.add_argument('ods', type=str, help='ods')
    args = parser.parse_args()

    # database
    print(f"making collocations from {args.ods}", file=sys.stderr)
    with ods_set(args.ods) as collocations:
        print(f"made collocations from {args.ods} {len(list(collocations))}", file=sys.stderr)


def main():
    run()


if __name__ == '__main__':
    main()
