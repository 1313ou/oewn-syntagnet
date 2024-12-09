#!/usr/bin/python3

import argparse
import sys

from oewn_core.deserialize import load_pickle
from oewn_core.serialize import save_pickle
from oewn_core.wordnet import Sense
from oewn_core.wordnet_fromyaml import load

pickle = None  # 'oewn.pickle'

inverse = False


def run():
    parser = argparse.ArgumentParser(description="load from YAML, scan collocations")
    parser.add_argument('repo', type=str, help='repository home')
    args = parser.parse_args()

    # run
    fast_load = pickle is not None
    src = 'pickle' if fast_load else f'{args.repo}'
    print(f"loading from {src}", file=sys.stderr)
    wn = load_pickle('.', file=f'{args.repo}.pickle') if fast_load else load(args.repo, extend=False)
    print(f"loaded from {args.repo}", file=sys.stderr)

    print(f"extending", file=sys.stderr)
    wn.extend()
    print(f"extended", file=sys.stderr)

    if not fast_load:
        save_pickle(wn, '.', file=f"{args.repo}")

    count = 0
    invcount = 0
    for s in wn.senses:
        for r in s.relations:
            if not r.other_type:
                t = Sense.Relation.Type(r.relation_type)
                if t == Sense.Relation.Type.COLLOCATION:
                    print(f"{s.id} > {r.target}", file=sys.stderr)
                    count += 1
                #elif t == Sense.Relation.Type.COLLOCATION_INV:
                #    print(f"{s.id} < {r.target}", file=sys.stderr)
                #    invcount += 1
    print(f"{count} collocations", file=sys.stderr)
    #print(f"{count} collocations {invcount} inverses", file=sys.stderr)


def main():
    run()


if __name__ == '__main__':
    main()
