# WARCraider - Convert WARC files into Avro for big data processing

## :warning: :construction: here be dragons
This is mostly developed on Windows and there are hardcoded URLs to download WARC files from Amazon S3 upload Avro files to Google Cloud Storage.

## Running
You'll need to download download tidy-html5 from https://github.com/htacg/tidy-html5/releases or your package manager.
You'll need clang, libclang-dev and llvm

There are a couple of options to run in production based on a 8 core/16 thread CPU. run.bat sets up environment variables for a single process but runall-4/8/16 will start that many processes that have CPU affinity sets to isolate each process to certain cores. 

After the avro files have be uploaded by this tool, there is an example on how to load them into Google BigQuery in load.bat