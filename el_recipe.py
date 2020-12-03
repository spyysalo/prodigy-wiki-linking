import datetime

import prodigy

from pathlib import Path
from logging import warning

from prodigy.util import set_hashes

from el_kb import TsvKnowledgeBase


def iso8601_now():
    """Return current time in ISO 8601 format w/o microseconds."""
    return datetime.datetime.now().replace(microsecond=0).isoformat(' ')


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
    stream = (make_prodigy_example(sent, span) for sent, span in ann_stream)
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
