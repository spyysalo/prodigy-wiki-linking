#!/usr/bin/env python3

import sys

from os.path import join
from argparse import ArgumentParser

from tqdm import tqdm
from sqlitedict import SqliteDict

from el_kb import load_aliases, load_descriptions, load_titles, load_counts


def argparser():
    ap = ArgumentParser()
    ap.add_argument('indir', help='directory with wiki csv data')
    ap.add_argument('dbname', help='database name')
    return ap


def main(argv):
    args = argparser().parse_args(argv[1:])
    aliases = load_aliases(join(args.indir, 'entity_alias.csv'))
    descs = load_descriptions(join(args.indir, 'entity_descriptions.csv'))
    titles = load_titles(join(args.indir, 'entity_defs.csv'))
    counts = load_counts(join(args.indir, 'prior_prob.csv'), titles)

    # make sure each alias is included
    for qid, aliases in aliases.items():
        for alias in aliases:
            counts[alias][qid] += 0

    with SqliteDict(args.dbname) as db:
        for alias, qid_count in tqdm(counts.items()):
            data = {}
            for qid, count in qid_count.items():
                data[qid] = {
                    'count': count,
                    'title': titles.get(qid),
                    'description': descs.get(qid),
                }
            db[alias] = data
        print('committing ...')
        db.commit()

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
