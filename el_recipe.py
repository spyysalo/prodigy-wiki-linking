import os
import datetime

import prodigy

from collections import namedtuple
from glob import glob
from pathlib import Path
from logging import warning

from prodigy.util import set_hashes

from el_kb import TsvKnowledgeBase


Textbound = namedtuple('Textbound', 'id type start end text')


def iso8601_now():
    """Return current time in ISO 8601 format w/o microseconds."""
    return datetime.datetime.now().replace(microsecond=0).isoformat(' ')


def load_standoff(txt_fn, ann_fn):
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


def make_prodigy_example(text, span):
    # Construct a dictionary with the structure that prodigy expects
    example = {
        'text': text,
        'meta': { 'score': 1.0 }    # TODO add ID
    }
    # add _input_hash and _task_hash
    example = set_hashes(example)
    span = {
        'start': span.start,
        'end': span.end,
        'text': span.text,
        'label': span.type,
        'source': 'manual',
        'rank': 0,
        'score': 1.0,
        'input_hash': example['_input_hash'],
    }
    example['spans'] = [span]
    # update _task_hash
    example = set_hashes(example, overwrite=True)
    return example


def ann_stream(directory):
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
        text, spans = load_standoff(f'{fn}.txt', f'{fn}.ann')
        for span in spans:
            sentence, sent_span = get_span_sentence(text, span)
            example = make_prodigy_example(sentence, sent_span)
            yield example


def format_option(count, qid, label, desc):
    prefix = 'https://www.wikidata.org/wiki/'
    return f'<a href="{prefix}{qid}" target="_blank">{label}</a>: {desc}'


def add_options(stream, kb):
    for task in stream:
        for span in task['spans']:
            options = []
            for candidate in kb.candidates(span['text']):
                count, qid, label, desc = candidate
                options.append({
                    'id': qid,
                    'html': format_option(*candidate),
                })
            if not options:
                warning(f'no options for {span["text"]}, skipping...')
                continue
            options.append({ 'id': 'NIL_other', 'text': 'Not in options'})
            options.append( {'id': 'NIL_ambiguous', 'text': 'Need more context'})
            task['options'] = options
            yield task


@prodigy.recipe(
    'entity_linker.manual',
    dataset=('The dataset to use', 'positional', None, str),
    annotator=('Annotator name', 'positional', None, str),
    directory=('The source data directory', 'positional', None, Path),
    kb_directory=('Path to the KB', 'positional', None, Path),
)
def entity_linker_manual(dataset, annotator, directory, kb_directory):
    kb = TsvKnowledgeBase(kb_directory)
    stream = ann_stream(directory)
    stream = add_options(stream, kb)

    def before_db(examples):
        for e in examples:
            if 'created' not in e:
                e['created'] = iso8601_now()
            if 'annotator' not in e:
                e['annotator'] = annotator
        return examples

    return {
        'dataset': dataset,
        'stream': stream,
        'view_id': 'choice',
        'before_db': before_db,
    }


if __name__ == '__main__':
    el = entity_linker_manual('dummy-dataset', 'dummy-user', 'normtest', 'fiwiki-kb-filtered')
    for e in el['stream']:
        print(e)
