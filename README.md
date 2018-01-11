# HaloDB

HaloDB is a fast and simple, embedded, persistent key-value storage engine written in Java. 
HaloDB's design is inspired by that of Bitcask. Refer to [Bitcask document](http://basho.com/wp-content/uploads/2015/05/bitcask-intro.pdf) for a high level overview of HaloDB's design 

HaloDB was primarily developed as an embedded storage engine for a high-throughput, low latency 
key-value database that powers Yahoo's DSP, and hence all its design choices and optimizations were
for this specific use case. 

Since HaloDB was built for a specific use case, we don't make extravagant claims about its performance. 
If you are looking for a general purpose storage engine suited for a wide variety of workloads and data sizes then 
HaloDB is probably not for you, but if your use case is somewhere near ours' then we can claim
with reasonable confidence that HaloDB will outperform other storage engines. 

HaloDB comprises of two main components: a cache in memory which stores all the keys, and files on
the persistent layer which stores all the data. Since all the keys are stored in memory HaloDB
requires considerable amount of it. Currently, storing one billion 8 byte keys requires around 
100GB of memory which is stored in native memory, outside the Java heap. 

These are few other limitations/restrictions of HaloDB that you need to be aware of:
* HaloDB supports multiple reader threads but only a single writer thread.
* HaloDB also doesn't order keys and hence doesn't support range scans.
* It takes time to open a HaloDB instance as it needs to scan all the index files and build a cache of all keys. 
    (For our production workload with 150 million records this is usually around 3 minutes.)
* Key size is restricted to 128 bytes. 


#### Read, Write and Space amplification.
Read amplification in HaloDB is always 1—for a read request it needs to do just one disk lookup—hence it is well suited for read latency critical workloads. 
HaloDB provides a simple configuration option which can be changed to tune write amplification and space amplification, both of which trade-off with each other;
HaloDB has a background compaction thread which removes stale data from the DB. The percentage of stale data at which a file is compacted can be controlled. Increasing this value
will increase space amplification but will reduce write amplification. For example if the value is set to 50% then write amplification
and space amplification will be approximately 2 and 1.5 respectively. 

#### Crash recovery.
Write Ahead Logs (WAL) are usually used by databases for crash recovery. Since for HaloDB WAL _is the_ database crash recovery
is easier. 
  TODO: add more details. 
  
#### Key cache. 
All keys and associated metadata are stored in memory. Currently HaloDB takes around 100GB of memory for a billion keys. 
Storing this in the Java Heap is a non-starter for a performance critical storage engine. HaloDB solves this problem by storing 
all data in native memory, outside the heap. Each key in the cache takes an additional 41 bytes for the metadata. Also, each bucket
in the hash table requires 8 bytes, and the total number of such buckets depends on the number of keys.  
Memory fragmentation is an always an issue with native memory allocation. We use [jemalloc](http://jemalloc.net/) which 
provides substantial reduction in fragmentation. We are also currently working on a slab allocator to further reduce memory usage
and fragmentation. 

#### Delete operations. 
Delete operation in HaloDB works very differently from inserts. //TODO: add more details. 

### Benchmarks.
  TODO: add results. 
  
### Basic Operations. 
  TODO: prove link to file here.   
  
  