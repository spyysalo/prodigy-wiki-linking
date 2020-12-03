import sys
import os

from collections import defaultdict
from logging import error

from standoff import ann_stream


def load_aliases(fn):
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


def load_labels(fn):
    label_by_qid = {}
    with open(fn) as f:
        next(f)    # skip header
        for ln, l in enumerate(f, start=2):
            l = l.rstrip('\n')
            label, qid = l.split('|')
            assert qid not in label_by_qid, f'dup in {fn}: {qid}'
            label_by_qid[qid] = label
    print(f'loaded {len(label_by_qid)} labels from {fn}',
          file=sys.stderr)
    return label_by_qid


def load_counts(fn, label_by_qid):
    failed, total = 0, 0
    qid_by_label = { v: k for k, v in label_by_qid.items() }
    counts_by_alias = defaultdict(list)
    with open(fn) as f:
        next(f)    # skip header
        for ln, l in enumerate(f, start=2):
            l = l.rstrip('\n')
            alias, count, label = l.split('|')
            count = int(count)
            try:
                qid = qid_by_label[label]
                counts_by_alias[alias].append((count, qid))
            except KeyError:
                failed += 1
            total += 1
    print(f'load_counts: failed mapping for {failed}/{total}', file=sys.stderr)
    return counts_by_alias


class TsvKnowledgeBase:
    def __init__(self, directory):
        self.aliases = load_aliases(os.path.join(directory, 'entity_alias.csv'))
        self.descriptions = load_descriptions(
            os.path.join(directory, 'entity_descriptions.csv'))
        self.labels = load_labels(os.path.join(directory, 'entity_defs.csv'))
        self.counts = load_counts(os.path.join(directory, 'prior_prob.csv'),
                                  self.labels)

        self.qids_by_text = defaultdict(set)
        for qid, aliases in self.aliases.items():
            for alias in aliases:
                self.qids_by_text[alias].add(qid)
        for qid, label in self.labels.items():
            self.qids_by_text[label].add(qid)

    def candidates(self, string):
        result = []
        for qid in self.qids_by_text.get(string, []):
            try:
                label = self.labels[qid]
            except KeyError:
                error(f'missing label for {qid}')
                continue
            try:
                desc = self.descriptions[qid]
            except KeyError:
                error(f'missing description for {qid}')
                continue                
            count = 0
            for c, q in self.counts.get(string, []):
                if q == qid:
                    count = c
                    break
            result.append((count, qid, label, desc))
            result.sort(reverse=True)    # highest count first
        return result


def main(argv):
    kb = TsvKnowledgeBase('fiwiki-kb-filtered')
    stream = ann_stream('data/ann')
    for sent, span in stream:
        for c in kb.candidates(span.text):
            print(c)
        else:
            print('MISSED:', span.text)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
