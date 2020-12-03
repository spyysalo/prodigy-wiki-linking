import sys
import os
import re

from collections import defaultdict
from logging import warning

from sqlitedict import SqliteDict

from standoff import ann_stream


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
        matches = []
        for s in unique(self.variants(string)):
            matches.extend(self.exact_match_candidates(s))
        matches.sort(key=lambda c: c[0], reverse=True)    # descending by count
        seen, uniq = set(), []
        for count, qid, title, desc in matches:
            if title not in seen:
                uniq.append((count, qid, title, desc))
            seen.add(title)
        return uniq

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
        for title, data in self.db[string].items():
            qid, desc = data['qid'], data['description']
            result.append((data['count'], qid, title, desc))
        result.sort(key=lambda c: c[0], reverse=True)    # highest count first
        return result


def main(argv):
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
