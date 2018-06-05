# HaloDB Change Log

## 0.2.1 (6/5/2018)
* Introduced _cleanUpTombstonesDuringOpen_ option to cleanup stale tombstone records during db open.   

## 0.2.0 (5/31/2018)
* HaloDB public API methods now throw HaloDBException instead of IOException. 
* Change in the record format in data file; removed an unused byte flag. 
* Refactored HaloDBOptions class to use get/set methods. 