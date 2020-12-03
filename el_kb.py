import sys
import os
import re

from collections import defaultdict
from logging import warning

from sqlitedict import SqliteDict

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


def load_titles(fn):
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


def load_counts(fn, title_by_qid):
    failed, total = 0, 0
    qid_by_title = { v: k for k, v in title_by_qid.items() }
    counts = defaultdict(lambda: defaultdict(int))
    with open(fn) as f:
        next(f)    # skip header
        for ln, l in enumerate(f, start=2):
            l = l.rstrip('\n')
            alias, count, title = l.split('|')
            count = int(count)
            try:
                qid = qid_by_title[title]
                counts[alias][qid] += count
            except KeyError:
                failed += 1
            total += 1
    print(f'load_counts: failed mapping for {failed}/{total}', file=sys.stderr)
    return counts


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


class KnowledgeBase:
    def __init__(self, lemmafn):
        self.lemma_data = load_lemma_data(lemmafn)

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
        raise NotImplementedError


class SqliteKnowledgeBase(KnowledgeBase):
    def __init__(self, dbfn, lemmafn):
        super().__init__(lemmafn)
        self.db = SqliteDict(dbfn, flag='r')

    def exact_match_candidates(self, string):
        if string not in self.db:
            return []
        result = []
        for qid, data in self.db[string].items():
            title, desc = data['title'], data['description']
            if title is None:
                title = '[no title]'
            if desc is None:
                desc = '[no description]'
            result.append((data['count'], qid, title, desc))
        result.sort(reverse=True)    # highest count first
        return result


class TsvKnowledgeBase:
    def __init__(self, kbdir, lemmafn):
        super().__init__(lemmafn)
        self.aliases = load_aliases(os.path.join(kbdir, 'entity_alias.csv'))
        self.descriptions = load_descriptions(
            os.path.join(kbdir, 'entity_descriptions.csv'))
        self.titles = load_titles(os.path.join(kbdir, 'entity_defs.csv'))
        self.counts = load_counts(os.path.join(kbdir, 'prior_prob.csv'),
                                  self.titles)

        self.qids_by_text = defaultdict(set)
        for qid, aliases in self.aliases.items():
            for alias in aliases:
                self.qids_by_text[alias].add(qid)
        for qid, title in self.titles.items():
            self.qids_by_text[title].add(qid)

    def exact_match_candidates(self, string):
        result = []
        for qid in self.qids_by_text.get(string, []):
            try:
                title = self.titles[qid]
            except KeyError:
                warning(f'missing title for {qid}')
                title = '[no title]'
            try:
                desc = self.descriptions[qid]
            except KeyError:
                warning(f'missing description for {qid}')
                desc = '[no description]'
            count = 0
            for q, c in self.counts.get(string, {}).items():
                if q == qid:
                    count = c
                    break
            result.append((count, qid, title, desc))
        result.sort(reverse=True)    # highest count first
        return result


def main(argv):
    # kb = TsvKnowledgeBase('fiwiki-kb', 'data/fi-lemmas.tsv')
    kb = SqliteKnowledgeBase('fiwiki.sqlite', 'data/fi-lemmas.tsv')
    stream = ann_stream('data/ann')
    for sent, span in stream:
        candidates = kb.candidates(span.text)
        for c in candidates:
            print(span.text, c)
        if not candidates:
            print('MISSED:', span.text)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
