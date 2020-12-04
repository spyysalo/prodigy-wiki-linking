import datetime
import hashlib

import prodigy

from pathlib import Path
from logging import warning

from prodigy.components.filters import filter_duplicates

from standoff import ann_stream
from el_kb import SqliteKnowledgeBase


def iso8601_now():
    """Return current time in ISO 8601 format w/o microseconds."""
    return datetime.datetime.now().replace(microsecond=0).isoformat(' ')


def _hash(string):
    return int(hashlib.sha1(string.encode('utf-8')).hexdigest(), 16) % 2**31


def make_prodigy_example(docid, text, span):
    # Construct a dictionary with the structure that prodigy expects
    example = {
        'text': text,
        'meta': { 'score': 1.0 }    # TODO add ID
    }
    example['_input_hash'] = _hash(docid)
    span_dict = {
        'start': span.start,
        'end': span.end,
        'text': span.text,
        'label': span.type,
        'source': 'manual',
        'rank': 0,
        'score': 1.0,
        'input_hash': example['_input_hash'],
    }
    example['spans'] = [span_dict]
    # add _input_hash and _task_hash values required by prodigy
    example['_task_hash'] = _hash(f'{docid} {span.id} {span.type} {span.text}')
    return example


def format_option(count, qid, title, description):
    wp_prefix = 'https://fi.wikipedia.org/wiki/'
    wd_prefix = 'https://www.wikidata.org/wiki/'
    return ''.join([
        f'{title} ',
        f'<a href="{wp_prefix}{title}" target="_blank">[WP]</a> ',
        f'<a href="{wd_prefix}{qid}" target="_blank">[WD]</a> ' if qid else '',
        f'({count})',
        f': {description}' if description else ''
    ])


def add_options(stream, kb):
    for task in stream:
        for span in task['spans']:
            options = []
            for candidate in kb.candidates(span['text']):
                count, qid, title, desc = candidate
                options.append({
                    'id': title,
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
    kbpath=('Path to the KB', 'positional', None, Path),
)
def entity_linker_manual(dataset, annotator, directory, kbpath):
    kb = SqliteKnowledgeBase(kbpath, 'data/fi-lemmas.tsv')
    stream = ann_stream(directory)
    stream = (make_prodigy_example(*e) for e in stream)
    stream = add_options(stream, kb)
    stream = filter_duplicates(stream, by_input=False, by_task=True)

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
