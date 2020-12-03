import os

from glob import glob
from collections import namedtuple
from logging import warning


# Entity types to exlude by default from candidates
EXCLUDE_BY_DEFAULT = set([
    'DATE',
    'TIME',
    'CARDINAL',
    'ORDINAL',
    'QUANTITY',
    'PERCENT',
])


Textbound = namedtuple('Textbound', 'id type start end text')


def load_textbounds(txt_fn, ann_fn):
    with open(txt_fn) as txt_f:
        doc_text = txt_f.read()

    textbounds = []
    with open(ann_fn) as ann_f:
        for ln, l in enumerate(ann_f, start=1):
            l = l.rstrip('\n')
            if not l.startswith('T'):
                continue    # skip all but textbound annotations
            id_, type_span, text = l.split('\t')
            type_, start, end = type_span.split(' ')
            start, end = int(start), int(end)
            assert doc_text[start:end] == text
            textbounds.append(Textbound(id_, type_, start, end, text))
    return doc_text, textbounds


def get_span_sentence(text, span):
    # Return the sentence in which the given span occurs in the text.
    # Assumes that sentences are separated by newlines.
    offset = 0
    for sentence in text.split('\n'):
        if offset+len(sentence) > span.start:
            assert offset+len(sentence) >= span.end
            # Create span with adjusted text
            s, e = span.start - offset, span.end - offset
            sent_span = Textbound(span.id, span.type, s, e, span.text)
            assert sentence[sent_span.start:sent_span.end] == sent_span.text
            return sentence, sent_span
        offset += len(sentence) + 1


def ann_stream(directory, exclude=EXCLUDE_BY_DEFAULT):
    if exclude is None:
        exclude = set()

    # List .txt and .ann files
    txt_fns = glob(os.path.join(directory, '*.txt'))
    ann_fns = glob(os.path.join(directory, '*.ann'))
    # Grab unique for each without extension
    txt_fns = set(os.path.splitext(n)[0] for n in txt_fns)
    ann_fns = set(os.path.splitext(n)[0] for n in ann_fns)

    if txt_fns - ann_fns:
        warning(f'.txt files without .ann: {txt_fns-ann_fns}')
    if ann_fns - txt_fns:
        warning(f'.ann files without .txt: {ann_fns-txt_fns}')

    for fn in sorted(txt_fns & ann_fns):
        text, spans = load_textbounds(f'{fn}.txt', f'{fn}.ann')

        spans = [s for s in spans if s.type not in exclude]

        for span in spans:
            sentence, sent_span = get_span_sentence(text, span)
            yield sentence, sent_span
