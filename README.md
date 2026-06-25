# ⚠️ DEPRECATED / ARCHIVED

> **This project is no longer maintained.**
> Last release: v0.7 (July 2015) — 11 years ago.

Undark was a pioneering open-source tool for recovering deleted rows from SQLite3 databases by directly scanning the binary file format. It served its purpose well in the early 2010s, but has since been **technically superseded** by modern alternatives.

---

## Why Undark Is Obsolete

### Recovery Rate: Only 34.8%
In a standardized forensic benchmark (Nemetz, Schmitt, Freiling 2018 — 278 deleted records), Undark recovered only **95 records (34.8%)**, while the modern [FQLite](https://github.com/pawlaszczyk/fqlite) achieves **100%**.

### Critical Missing Features
- ❌ **No WAL (Write-Ahead Log) support** — SQLite's default journal mode since 2010
- ❌ **No rollback journal analysis** — misses historical data
- ❌ **No UTF-16 encoding support** — only UTF-8
- ❌ **No dropped table recovery**
- ❌ **No BLOB type detection** (images, protobuf, plists, etc.)
- ❌ **No auto page-size detection** for damaged headers

---

## Recommended Alternatives

### For forensic SQLite recovery (open source, free):
- **[FQLite](https://github.com/pawlaszczyk/fqlite)** (Java, Apache 2.0) — the gold standard. 100% recovery rate in benchmarks, WAL/journal support, multi-encoding, BLOB type inference, HTML export. Actively maintained (v4.0, Feb 2026).

### For basic deleted row recovery:
- **[sqlite-unhide](https://github.com/little-brother/sqlite-unhide)** — freeware console tool (1 MB size limit in free version)
- **[bring2lite](https://github.com/bring2lite/bring2lite)** — 52.9% recovery in benchmarks

### For corrupted database repair:
- **[sqlite3 `.recover` command](https://sqlite.org/recovery.html)** — built into the official SQLite CLI

---

## Historical Context

Undark was originally written by [Paul L Daniels](http://pldaniels.com/undark/) in 2013. At the time, it was one of very few open-source tools capable of recovering deleted SQLite data. Its byte-level scanning approach (no dependency on the SQLite library) allowed it to work even on corrupted files — a genuinely innovative approach for its era.

This fork (`alitrack/undark`) added Windows/MinGW build support and minor fixes. It was archived in 2026 as the forensic landscape has moved on.

---

## Original README (preserved below)

**about Undark**

Undark is a tool that will go through your entire SQLite database file and dump out all rows of data it finds still intact ( both current and deleted rows ). Undark does not differentiate between current and deleted data. The output of Undark is plain text CSV format.

this project is forked from [Undark](http://pldaniels.com/undark/)

Original Author: [Paul L Daniels](mailto:pldaniels@pldaniels.com)

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
