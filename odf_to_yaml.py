#!/usr/bin/python3

import argparse
import os
import sys
from contextlib import contextmanager
from typing import Set, Dict, List, Tuple

import ezodf
from oewn_core.deserialize import load_pickle
from oewn_core.wordnet import Sense
from oewn_core.wordnet_fromyaml import load
from oewn_core.wordnet_toyaml import save

import ods_columns as cols
from oewn_core.wordnet import WordnetModel

fastLoad = True

inverse = True

SOURCE = 1

TARGET = 2


# C O L L O C A T I O N   D I C T   F R O M   O D F


def read_row(sheet):
    for row in range(sheet.nrows()):
        yield [sheet[row, col] for col in range(sheet.ncols())]


@contextmanager
def ods_collocations(filepath: str):
    file_abspath = os.path.abspath(filepath)
    doc = ezodf.opendoc(file_abspath)
    m = make_collocations(doc.sheets[0])
    try:
        yield m
    finally:
        m.clear()


def make_collocations(sheet) -> Dict[str, List[Tuple[int, str]]]:
    """
    Build collocations from ODS spreadsheet
    :param sheet: ODS sheet
    :return: Dict[sensekey1, List[(1,sensekey2a), (2,sensekey2b)]
    """
    collocations = dict()
    for i, row in enumerate(read_row(sheet)):
        sensekey1 = row[cols.sensekey1_col].value
        sensekey2 = row[cols.sensekey2_col].value

        # filter reflexive collocations
        if sensekey1 == sensekey2:
            print(f'{i + 1} {sensekey1} - {sensekey1} reflexive', file=sys.stderr)
            continue

        # source to target
        if sensekey1 not in collocations:
            collocations[sensekey1] = []
        collocations[sensekey1].append((sensekey2, TARGET))

        # target to source
        if inverse:
            if sensekey2 not in collocations:
                collocations[sensekey2] = []
            collocations[sensekey2].append((sensekey1, SOURCE))
    return collocations


def generate_collocated(collocations: Dict[str, List[Tuple[int, str]]]):
    for sk in collocations:
        for sk2 in collocations[sk]:
            yield sk2


# C O L L E C T   S E N S E K E Y S   F R O M   M O D E L

def make_sensekeys(wn: WordnetModel) -> Set[str]:
    sensekeys: Set = set()
    for s in wn.senses:
        sensekeys.add(s.id)
    return sensekeys


# A D D   C O L L O C A T I O N S  T O   M O D E L

def process_sense(sense, collocations, sensekeys):
    count = 0
    fails = 0
    sk = sense.id
    if sk in collocations:
        for sk2, direction in collocations[sk]:
            # check if target is resolvable
            if sk2 not in sensekeys:
                print(f'{sk2} target not resolvable in collocation {sk}-{sk2}', file=sys.stderr)
                fails += 1
                continue
            # type
            t = Sense.Relation.Type.COLLOCATION if direction == TARGET else Sense.Relation.Type.COLLOCATION_INV

            # add
            if sense.relations is None:
                sense.relations = []
            # print(f"add {sk}-{t}->{sk2}")
            sense.relations.append(Sense.Relation(sk2, t))
            count += 1
    return count, fails


def process_senses(wn, collocations):
    sensekeys: Set[str] = make_sensekeys(wn)

    count = 0
    fails = 0
    for sense in wn.senses:
        scount, sfails = process_sense(sense, collocations, sensekeys)
        count += scount
        fails += sfails
    return count, fails


def run():
    parser = argparse.ArgumentParser(description="load from YAML, process using ODS and write")
    parser.add_argument('ods', type=str, help='ods')
    parser.add_argument('repo', type=str, help='repository home')
    parser.add_argument('out_repo', type=str, help='output repo')
    args = parser.parse_args()

    # database
    print(f"making collocations from {args.ods}", file=sys.stderr)
    with ods_collocations(args.ods) as collocations:
        print(f"made collocations from {args.ods} {len(list(generate_collocated(collocations)))}", file=sys.stderr)
        # run
        src = 'pickle' if fastLoad else f'{args.repo}'
        print(f"loading from {src}", file=sys.stderr)
        wn = load_pickle('.', 'oewn.pickle') if fastLoad else load(args.repo, extend=False)
        print(f"loaded from {args.repo}", file=sys.stderr)
        # process
        print(f"processing", file=sys.stderr)
        n, f = process_senses(wn, collocations)
        print(f"processed {n} collocations were added, {f} failed", file=sys.stderr)
        # write
        print(f"saving to {args.out_repo}", file=sys.stderr)
        save(wn, args.out_repo)
        print(f"saved to {args.out_repo}", file=sys.stderr)


def main():
    run()


if __name__ == '__main__':
    main()
