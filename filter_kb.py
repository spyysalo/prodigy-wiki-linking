#!/usr/bin/env python3

import sys
import os

from collections import defaultdict
from argparse import ArgumentParser

from el_kb import load_aliases, load_descriptions, load_titles, load_counts


def save_aliases(fn, aliases_by_qid):
    with open(fn, 'w') as f:
        print('WD_id|alias', file=f)    # header
        for qid, aliases in aliases_by_qid.items():
            for alias in aliases:
                print(f'{qid}|{alias}', file=f)


def save_descriptions(fn, desc_by_qid):
    with open(fn, 'w') as f:
        print('WD_id|description', file=f)    # header
        for qid, desc in desc_by_qid.items():
            print(f'{qid}|{desc}', file=f)


def save_titles(fn, title_by_qid):
    with open(fn, 'w') as f:
        print('WP_title|WD_id', file=f)    # header
        for qid, title in title_by_qid.items():
            print(f'{title}|{qid}', file=f)


def save_counts(fn, counts_by_alias, title_by_qid):
    with open(fn, 'w') as f:
        print('alias|count|entity', file=f)    # header
        for alias, counts in counts_by_alias.items():
            for count, qid in counts:
                title = title_by_qid[qid]
                print(f'{alias}|{count}|{title}', file=f)


def argparser():
    ap = ArgumentParser()
    ap.add_argument('indir', help='KB directory')
    ap.add_argument('outdir', help='output directory')
    ap.add_argument('-m', '--min-count', type=int, default=2)
    return ap


def main(argv):
    args = argparser().parse_args(argv[1:])
    aliases = load_aliases(os.path.join(args.indir, 'entity_alias.csv'))
    descriptions = load_descriptions(
        os.path.join(args.indir, 'entity_descriptions.csv'))
    titles = load_titles(os.path.join(args.indir, 'entity_defs.csv'))
    counts = load_counts(os.path.join(args.indir, 'prior_prob.csv'), titles)

    filtered_qids = set()
    filtered_counts = defaultdict(list)
    filtered, total = 0, 0
    for alias, count_list in counts.items():
        for count, qid in count_list:
            if count >= args.min_count:
                filtered_counts[alias].append((count, qid))
                filtered_qids.add(qid)
            else:
                filtered += 1
            total += 1
    print(f'filtered {filtered}/{total} ({filtered/total:.1%}) counts',
          file=sys.stderr)

    filtered_titles = {}
    filtered, total = 0, 0
    for qid, title in titles.items():
        if qid in filtered_qids:
            filtered_titles[qid] = title
        else:
            filtered += 1
        total += 1
    print(f'filtered {filtered}/{total} ({filtered/total:.1%}) titles',
          file=sys.stderr)

    filtered_descs = {}
    filtered, total = 0, 0
    for qid, desc in descriptions.items():
        if qid in filtered_qids:
            filtered_descs[qid] = desc
        else:
            filtered += 1
        total += 1
    print(f'filtered {filtered}/{total} ({filtered/total:.1%}) descriptions',
          file=sys.stderr)

    filtered_aliases = defaultdict(list)
    filtered, total = 0, 0
    for qid, alias_list in aliases.items():
        for alias in alias_list:
            if qid in filtered_qids:
                filtered_aliases[qid].append(alias)
            else:
                filtered += 1
            total += 1
    print(f'filtered {filtered}/{total} ({filtered/total:.1%}) aliases',
          file=sys.stderr)

    save_aliases(os.path.join(args.outdir, 'entity_alias.csv'),
                 filtered_aliases)
    save_descriptions(os.path.join(args.outdir, 'entity_descriptions.csv'),
                      filtered_descs)
    save_titles(os.path.join(args.outdir, 'entity_defs.csv'),
                filtered_titles)
    save_counts(os.path.join(args.outdir, 'prior_prob.csv'),
                filtered_counts, titles)
    
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
