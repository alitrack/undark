**about Undark**

Undark is a tool that will go through your entire SQLite database file and dump out all rows of data it finds still intact ( both current and deleted rows ). Undark does not differentiate between current and deleted data. The output of Undark is plain text CSV format.

this project is forked from [Undark v0.6](http://pldaniels.com/undark/)

**What Undark can do:**
- Retrieve most available records from a SQLite3 DB and dump them to stdout
- Dump normal (visible) records to stdout
- Dump deleted (unvacuumed) records to stdout
- Retrieve data from corrupted SQLite DBs (because it only examines data on a per record basis)

**What Undark can't do:**
- Recover data that's already been vacuumed out of the file
- Magically put the records back in to your db file
 
**usage:**
```
undark -i <sqlite DB> [-d] [-v] [-V|--version]
	[--cellcount-min=<count>] [--cellcount-max=<count>] 
	[--rowsize-min=<bytes>] [--rowsize-max=<bytes>]
	[--no-blobs] [--blob-size-limit=<bytes>]
	[--fine-search]
        -i: input SQLite3 format database
        -d: enable debugging output (very large dumps)
        -v: enable verbose output
        -V|--version: show version of software
        -h|--help: show this help
        --cellcount-min: define the minimum number of cells a row must have to be extracted
        --cellcount-max: define the maximum number of cells a row must have to be extracted
        --rowsize-min: define the minimum number of bytes a row must have to be extracted
        --rowsize-max: define the maximum number of bytes a row must have to be extracted
        --no-blobs: disable the dumping of blob data
        --blob-size-limit: all blobs larger than this size are dumped to .blob files
        --fine-search: search DB shifting one byte at a time, rather than records
```

**Example usage:**
```
./undark -i sms.db > sms-data.csv
'8079','C6CA760C-948C-4CDC-86B2-85D527C8E523','Fingers crossed','0',NULL,'47',NULL,NULL,'blob','10','0','iMessage'
'8076','D5F4356C-F0F4-4507-B767-587627709C5F','Did u remind the kids I''m picking them up this afternoon?','0',NULL,'47',NULL,NULL,'blob','10','0','iMessage'
```
