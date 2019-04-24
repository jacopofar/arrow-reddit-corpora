from bz2 import BZ2File
import re

import click
import ujson
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

comment_schema = pa.schema([
    ('created_utc', pa.int64()),
    ('subreddit_id', pa.string()),
    ('link_id', pa.string()),
    ('id', pa.string()),
    ('author', pa.string()),
    ('score_hidden', pa.bool_()),
    ('body', pa.string()),
    ('edited', pa.int64()),
    ('archived', pa.bool_()),
    ('name', pa.string()),
    ('retrieved_on', pa.int64()),
    ('author_flair_css_class', pa.string()),
    ('ups', pa.int32()),
    ('controversiality', pa.int32()),
    ('score', pa.int32()),
    ('subreddit', pa.string()),
    ('author_flair_text', pa.string()),
    ('parent_id', pa.string()),
    ('distinguished', pa.string()),
    ('gilded', pa.int32()),
    ('downs', pa.int32()),
])


def validate_subreddit_list(ctx, param, value):
    """Validate and return a list of subreddits from comma-separated argument.

    Takes care of removing trailing spaces and lowercasing.

    Parameters
    ----------
    ctx : Click context
        not used
    param : Click param
        not used
    value : str
        The string from the CLI argument

    Returns
    -------
    list of str
        The list of subreddits

    Raises
    ------
    click.BadParameter
        An error reporting which subreddit name was invalid
    """
    if value is None:
        return None
    subs = [sub.strip().lower() for sub in value.split(',')]
    # https://github.com/reddit-archive/reddit/blob/753b17407e9a9dca09558526805922de24133d53/r2/r2/models/subreddit.py#L114
    subreddit_rx = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_]{2,20}\Z")
    for sub in subs:
        if not subreddit_rx.match(sub):
            raise click.BadParameter(f'Invalid subreddit name: {sub}')
    return subs


def write_batch_to_parquet(pending_arrays, writer):
    arrays = []
    for name in comment_schema.names:
        arrays.append(pa.array(
            pending_arrays[name],
            type=comment_schema.field_by_name(name).type))
        pending_arrays[name] = []

    b = pa.RecordBatch.from_arrays(arrays, comment_schema.names)
    t = pa.Table.from_batches([b])
    writer.write_table(t)


@click.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('output_file', type=click.Path())
@click.option('-subreddits', default=None, callback=validate_subreddit_list)
def process(input_file, output_file, subreddits):
    # data to be appended, organized by columns
    pending_arrays = {name: [] for name in comment_schema.names}
    pending_rows = 0
    write_every = 100
    # ballpark estimation of the total, just for a reference
    pbar = tqdm(total=50_000_000)
    with BZ2File(input_file, 'r') as f, \
            pq.ParquetWriter(output_file, comment_schema) as writer:
        for line in f:
            # ujson parses JSON fast and also directly from binary :)
            obj = ujson.loads(line)
            if subreddits is not None:
                if obj['subreddit'].lower() not in subreddits:
                    continue
            # append all the values
            for k, v in obj.items():
                # some conversions
                if k == 'created_utc':
                    v = int(v)
                if k == 'edited':
                    # it's a number or False (??), adjust it
                    if not v:
                        v = None
                if k not in pending_arrays:
                    print(f'The key {k} is not known, had value {v}')
                    continue
                pending_arrays[k].append(v)
            pending_rows += 1
            if pending_rows % write_every == 0:
                write_batch_to_parquet(pending_arrays, writer)
                pbar.update(write_every)
        # last write
        write_batch_to_parquet(pending_arrays, writer)
    pbar.close()
    print(f'Wrote a total of {pending_rows} entries to parquet')


if __name__ == '__main__':
    process()

