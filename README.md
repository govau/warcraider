# WARCraider - Convert WARC files into Avro for big data processing

## :warning: :construction: here be dragons
This is mostly developed on Windows and there are hardcoded URLs to download WARC files from Amazon S3 upload Avro files to Google Cloud Storage.

## Running
You'll need to download download tidy-html5 from https://github.com/htacg/tidy-html5/releases or your package manager (homebrew on OSX).

There are a couple of options to run in production based on a 8 core/16 thread CPU. run.bat sets up environment variables for a single process but runall-4/8/16 will start that many processes that have CPU affinity sets to isolate each process to certain cores. 

After the avro files have be uploaded by this tool, there is an example on how to load them into Google BigQuery in load.bat

## How it works
- Each instance takes in environment variables about how many other instances there are (REPLICAS), what number to start from (OFFSET) and command line argument which instance number it is 
- The program will check Google Cloud Storage to see if a completed Avro file exists for that number and download the next numbered WARC file from S3 if not
- The program will process each record of the WARC in two 50,000 record batches
- The program will check each record is smaller than 2MB and is not on a blacklist else it will write a stub record for that record URL
- The program will try to parse the HTML of the record and if it doesn't work, will try to fix errors with regex and HTML Tidy and after that will extract things like text and links using regex
- The program will take the extracted text and run further algorithms like keyword extraction and word count
- The program will save each record into the Avro file
- The program will upload the completed Avro file to Google storage
- The program will increment the counter by REPLICAS and run again for the next WARC file