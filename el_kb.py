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
        self.aliases = load_wd_aliases(os.path.join(kbdir, 'entity_alias.csv'))
        self.descriptions = load_descriptions(
            os.path.join(kbdir, 'entity_descriptions.csv'))
        self.titles = load_title_wdid_mapping(
            os.path.join(kbdir, 'entity_defs.csv'))
        self.counts = load_counts(os.path.join(kbdir, 'prior_prob.csv'))

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
