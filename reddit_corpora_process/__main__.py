from bz2 import BZ2File

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


@click.command()
@click.argument('input_file', type=click.Path(exists=True))
def process(input_file):
    # data to be appended, organized by columns
    pending_arrays = {name: [] for name in comment_schema.names}
    pending_rows = 0
    write_every = 10000
    pbar = tqdm(total=48807699)
    with BZ2File(input_file, 'r') as f, \
         pq.ParquetWriter('example3.parquet', comment_schema) as writer:
        for line in f:
            # ujson parses JSON fast and also directly from binary :)
            obj = ujson.loads(line)
            # append all the values
            for k, v in obj.items():
                # some conversions
                if k == 'created_utc':
                    v = int(v)
                if k == 'edited':
                    # it's a number or False (??), adjust it
                    if not v:
                        v = None
                pending_arrays[k].append(v)
            pending_rows += 1
            if pending_rows % write_every == 0:
                arrays = []
                for name in comment_schema.names:
                    arrays.append(pa.array(
                        pending_arrays[name],
                        type=comment_schema.field_by_name(name).type))
                    pending_arrays[name] = []

                b = pa.RecordBatch.from_arrays(arrays, comment_schema.names)
                t = pa.Table.from_batches([b])
                writer.write_table(t)
                pbar.update(write_every)

    pbar.close()



if __name__ == '__main__':
    process()