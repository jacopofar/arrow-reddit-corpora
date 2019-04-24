Tool to ingest the Reddit message corpora and produce a Parquet file based on Arrow and with Snappy compression.

Mainly intended as a test for Arrow and its tooling

## Run
Download one month of data at least and use `make run`. It will try to process the file `RC_2014-12.bz2` and save all the posts in parquet, then do the same filtering for subreddits in Italian language.

## Some stats
The file for Reddit comments for December, 2014, contains a bit less than 50 millions of comments.
Such file takes is compressed with bz2 and takes 4.6 GB (around 20GB uncompressed).
The corresponding parquet file takes 7.6 GB.
The one filtered only for subreddits in Italian is 5MB and contains 19K comments
