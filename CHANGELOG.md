# HaloDB Change Log

## 0.4.3 (08/20/2018)
* Sequence number, instead of relying on system time, is now a number incremented for each write operation. 
* Include compaction rate in stats.  

## 0.4.2 (08/06/2018)
* Handle the case where db crashes while it is being repaired due to error from a previous crash.
* _put_ operation in _HaloDB_ now returns a boolean value indicating the status of the operation.

## 0.4.1 (7/16/2018)
* Include version, checksum and max file size in META file. 
* _maxFileSize_ in _HaloDBOptions_ now accepts only int values.  

## 0.4.0 (7/11/2018)
* Implemented memory pool for in-memory index. 

