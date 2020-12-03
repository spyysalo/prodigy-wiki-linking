import sys
import os
import re

from collections import defaultdict
from logging import warning

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


def load_lemma_data(fn):
    lemma_data = defaultdict(list)
    with open(fn) as f:
        for ln, l in enumerate(f, start=1):
            l = l.rstrip('\n')
            if l.startswith('#'):
                continue    # skip comments
            count, form, lemma, pos = l.split('\t')
            # assume '#' is used to join compound words and remove it
            # when it appears between two alphabetic characters
            lemma = re.sub(r'(?<=[^\W\d])#(?=[^\W\d])', r'', lemma)
            lemma_data[form].append((lemma, pos, count))
    return lemma_data


def unique(sequence):
    """Return unique items in sequence, preserving order."""
    # https://www.peterbe.com/plog/fastest-way-to-uniquify-a-list-in-python-3.6
    return list(dict.fromkeys(sequence))


class TsvKnowledgeBase:
    def __init__(self, kbdir, lemmafn):
        self.aliases = load_aliases(os.path.join(kbdir, 'entity_alias.csv'))
        self.descriptions = load_descriptions(
            os.path.join(kbdir, 'entity_descriptions.csv'))
        self.labels = load_labels(os.path.join(kbdir, 'entity_defs.csv'))
        self.counts = load_counts(os.path.join(kbdir, 'prior_prob.csv'),
                                  self.labels)
        self.lemma_data = load_lemma_data(lemmafn)

        self.qids_by_text = defaultdict(set)
        for qid, aliases in self.aliases.items():
            for alias in aliases:
                self.qids_by_text[alias].add(qid)
        for qid, label in self.labels.items():
            self.qids_by_text[label].add(qid)

    def lemmatize_last(self, words):
        for lemma, pos, count in self.lemma_data.get(words[-1], []):
            yield words[:-1] + [lemma]

    def variants(self, string):
        words = string.split(' ')    # assume space-separated
        for lemmatized in self.lemmatize_last(words):
            yield ' '.join(lemmatized)
        yield string

    def candidates(self, string):
        for s in unique(self.variants(string)):
            return self.exact_match_candidates(s)

    def exact_match_candidates(self, string):
        result = []
        for qid in self.qids_by_text.get(string, []):
            try:
                label = self.labels[qid]
            except KeyError:
                warning(f'missing label for {qid}')
                label = '[no label]'
            try:
                desc = self.descriptions[qid]
            except KeyError:
                warning(f'missing description for {qid}')
                desc = '[no description]'
            count = 0
            for c, q in self.counts.get(string, []):
                if q == qid:
                    count = c
                    break
            result.append((count, qid, label, desc))
            result.sort(reverse=True)    # highest count first
        return result


def main(argv):
    kb = TsvKnowledgeBase('fiwiki-kb', 'data/fi-lemmas.tsv')
    stream = ann_stream('data/ann')
    for sent, span in stream:
        candidates = kb.candidates(span.text)
        for c in candidates:
            print(span.text, c)
        if not candidates:
            print('MISSED:', span.text)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
