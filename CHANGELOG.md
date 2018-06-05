# HaloDB Change Log

## 0.2.0 (5/31/2018)
* HaloDB public API methods now throw HaloDBException instead of IOException. 
* Change in the record format in data file; removed an unused byte flag. 
* Refactored HaloDBOptions class to use get/set methods. 