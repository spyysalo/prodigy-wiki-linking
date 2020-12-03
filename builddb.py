#!/usr/bin/env python3

import sys

from os.path import join
from collections import defaultdict
from argparse import ArgumentParser

from tqdm import tqdm
from sqlitedict import SqliteDict


def load_wd_aliases(fn):
    """
    Load mapping from Wikidata QIDs to Wikidata aliases.
    """
    total = 0
    aliases_by_qid = defaultdict(list)
    with open(fn) as f:
        next(f)    # skip header
        for ln, l in enumerate(f, start=2):
            l = l.rstrip('\n')
            qid, alias = l.split('|', 1)
            aliases_by_qid[qid].append(alias)
            total += 1
    print(f'loaded {total} aliases for {len(aliases_by_qid)} IDs from {fn}',
          file=sys.stderr)
    return aliases_by_qid


def load_title_by_qid(fn):
    """
    Load mapping from Wikipedia titles to Wikidata QIDs.
    """
    title_by_qid = {}
    with open(fn) as f:
        next(f)    # skip header
        for ln, l in enumerate(f, start=2):
            l = l.rstrip('\n')
            title, qid = l.split('|')
            assert qid not in title_by_qid, f'dup in {fn}: {qid}'
            title_by_qid[qid] = title
    print(f'loaded {len(title_by_qid)} titles from {fn}',
          file=sys.stderr)
    return title_by_qid


def load_descriptions(fn):
    desc_by_qid = {}
    with open(fn) as f:
        next(f)    # skip header
        for ln, l in enumerate(f, start=2):
            l = l.rstrip('\n')
            qid, description = l.split('|', 1)
            assert qid not in desc_by_qid, f'dup in {fn}: {qid}'
            desc_by_qid[qid] = description
    print(f'loaded {len(desc_by_qid)} descriptions from {fn}',
          file=sys.stderr)
    return desc_by_qid


def filter_title(title):
    excluded_prefixes = [
        'Käyttäjä:',
        'Toiminnot:',
        'Metasivu:',
        'Luokka:',
        ':Luokka:'
    ]
    if any(title.startswith(p) for p in excluded_prefixes):
        return True
    return False


def load_counts(fn):
    filtered, total = 0, 0
    counts = defaultdict(lambda: defaultdict(int))
    with open(fn) as f:
        next(f)    # skip header
        for ln, l in enumerate(f, start=2):
            l = l.rstrip('\n')
            alias, count, title = l.split('|')
            count = int(count)
            if filter_title(title):
                filtered += 1
            else:
                counts[alias][title] += count
            total += 1
    print(f'filtered {filtered}/{total} titles from {fn}', file=sys.stderr)
    print(f'loaded counts for {len(counts)} strings from {fn}', file=sys.stderr)
    return counts


def argparser():
    ap = ArgumentParser()
    ap.add_argument('indir', help='directory with wiki csv data')
    ap.add_argument('dbname', help='database name')
    return ap


def main(argv):
    args = argparser().parse_args(argv[1:])
    aliases = load_wd_aliases(join(args.indir, 'entity_alias.csv'))
    descs = load_descriptions(join(args.indir, 'entity_descriptions.csv'))
    title_by_qid = load_title_by_qid(join(args.indir, 'entity_defs.csv'))
    counts = load_counts(join(args.indir, 'prior_prob.csv'))

    qid_by_title = { v: k for k, v in title_by_qid.items() }

    # make sure each WD alias is included
    for qid, aliases in aliases.items():
        if qid not in title_by_qid:
            continue    # unmappable
        for alias in aliases:
            counts[alias][title_by_qid[qid]] += 0

    with SqliteDict(args.dbname) as db:
        for string, title_count in tqdm(counts.items()):
            data = {}
            for title, count in title_count.items():
                qid = qid_by_title.get(title)
                data[title] = {
                    'count': count,
                    'qid': qid,
                    'description': descs.get(qid),
                }
            db[string] = data
        print('committing ...')
        db.commit()

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
