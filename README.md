# HaloDB

HaloDB is a fast, simple, embedded, persistent key-value storage engine written in Java.
Basic design principles employed in HaloDB were inspired by those of log structured file systems and similar ideas can be found in systems  
like [BitCask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf), [Aerospike](https://www.aerospike.com/docs/architecture), and [Haystack](https://code.facebook.com/posts/685565858139515/needle-in-a-haystack-efficient-storage-of-billions-of-photos/).  

HaloDB was designed and developed as an embedded storage engine for a high-throughput, low latency 
distributed key-value database that powers Yahoo's DSP, and hence all its design choices and optimizations were
primarily for this use case.  

HaloDB comprises of two main components: an index in memory which stores all the keys, and files on
the persistent layer which stores all the data. To reduce Java garbage collection pressure the index 
is allocated in native memory, outside the Java heap. 

![HaloDB](https://lh3.googleusercontent.com/JxnA8Kznn2jZrG1ytAEo5OZ5OCrnfhDOaLfK1D30SZSz_Dl1IU2666fDc8lGGBW1zGSEjOBWw07C5eltiSmOvxc34OhO0nzqaSxGzE-AeVS8gihFp4E8NBFOnzmjkfsPsQO69x5x2Q=w1576-h872-no)

The workload for which HaloDB was designed for has the following characteristics:
* Small keys(8 bytes) and large values (10Kb average)
* Both read and write throughput are high.
* Low lookup latency SLA of < 1ms at the 99th percentile. 
* Single writer thread.
* No need for ordered access or range scans.
* Data size is significantly larger than the available memory, hence workload is IO bound. 

If your workload is similar HaloDB is probably a good choice for you.     

HaloDB also has the following limitations/restrictions which you should be aware of: 
* Since all the keys are stored in memory HaloDB need lots of it. Currently for storing 100 million 8 byte keys 
   HaloDB needs around 8GB of memory.  
* HaloDB supports multiple reader threads but only a single writer thread.
* HaloDB doesn't order keys and hence doesn't support range scans.
* It takes time to open a HaloDB instance as it needs to scan all the index files and build a cache of all keys. 
    (For our production workload with 150 million records in each db this usually around 3 minutes.)
* Key size is restricted to 127 bytes. 

### Basic Operations. 
```java
    // Open a db with default options. 
    HaloDBOptions options = new HaloDBOptions();
    
    // size of each data file size will be 1GB.
    options.maxFileSize = 1024 * 1024 * 1024;

    // the threshold at which page cache is synced to disk.
    // data will be durable only if it is flushed to disk, therefore
    // more data will be lost if this value is set too high. Setting
    // this value too low might interfere with read performance.
    options.flushDataSizeBytes = 10 * 1024 * 1024;

    // The percentage of stale data in a data file at which the file will be compacted.
    // This value helps control write and space amplification. Increasing this value will
    // reduce write amplification but will increase space amplification.
    // This along with the compactionJobRate below is the most important setting 
    // for tuning HaloDB performance.
    options.compactionThresholdPerFile = 0.75;

    // Controls how fast the compaction job should run.
    // This is the amount of data which will be copied by the compaction thread per second.
    // Optimal value depends on the compactionThresholdPerFile option. 
    options.compactionJobRate = 50 * 1024 * 1024;

    // Setting this value is important as it helps to preallocate enough
    // memory for the off-heap cache. If the value is too low the db might
    // need to rehash the cache. For a db of size n set this value to 2*n.
    options.numberOfRecords = 100_000_000;


    // Represents a database instance and provides all methods for operating on the database. 
    HaloDB db = null;

    // The directory will be created if it doesn't exist and all database files will be stored in this directory
    String directory = "directory";

    // Open the database. Directory will be created if it doesn't exist.
    // If we are opening an existing database HaloDB needs to scan all the
    // index files to create the in-memory index, which, depending on the db size, might take a few minutes. 
    db = HaloDB.open(directory, options);

    // key and values are byte arrays. Key size is restricted to 128 bytes.
    byte[] key1 = Ints.toByteArray(200);
    byte[] value1 = "Value for key 1".getBytes();

    byte[] key2 = Ints.toByteArray(300);
    byte[] value2 = "Value for key 2".getBytes();

    // add the key-value pair to the database.
    // HaloDB doesn't support cocurrent writes. 
    // This is expected to be enforced by the calling application.
    db.put(key1, value1);
    db.put(key2, value2);

    // read the value from the database. 
    value1 = db.get(key1);
    value2 = db.get(key2);

    // delete a key from the database.
    db.delete(key1);

    // Open an iterator and iterate through all the key-value records.
    HaloDBIterator iterator = db.newIterator();
    while (iterator.hasNext()) {
        Record record = iterator.next();
        System.out.println(Ints.fromByteArray(record.getKey()));
        System.out.println(new String(record.getValue()));
    }
    
    // get stats and print it. 
    HaloDBStats stats = db.stats();
    System.out.println(stats.toString());

    // reset stats
    db.resetStats();
    
    // Close the database.
    db.close();    

```   



### Read, Write and Space amplification.
Read amplification in HaloDB is always 1—for a read request it needs to do just one disk lookup—hence it is well suited for read latency critical workloads. 
HaloDB provides a simple configuration option which can be changed to tune write amplification and space amplification, both of which trade-off with each other;
HaloDB has a background compaction thread which removes stale data from the DB. The percentage of stale data at which a file is compacted can be controlled. Increasing this value
will increase space amplification but will reduce write amplification. For example if the value is set to 50% then write amplification will be approximately 2. 


### Durability and Crash recovery.
Write Ahead Logs (WAL) are usually used by databases for crash recovery. Since for HaloDB WAL _is the_ database crash recovery
is easier and faster. 

HaloDB does not flush writes to disk immediately, but, for performance reasons, writes only to the OS page cache. The cache is synced to 
disk once a configurable size is reached. In the event of a power loss, the data not flushed to disk will be lost. This compromise
between performance and durability is a necessary one. 

In the event of a power loss and data corruption, HaloDB will scan and discard corrupted records. Since the write thread and compaction 
thread could be writing to at most two files at a time only those files need to be repaired and hence recovery times are very short.

In the event of a power loss HaloDB offers the following consistency guarantees:
* Writes are atomic.
* Inserts and updates are committed to disk in the same order they are received.
* When inserts/updates and deletes are interleaved total ordering is not guaranteed, but partial ordering is guaranteed for inserts/updates and deletes.    
 
  
### In-memory index.  
HaloDB stores all keys and associated metadata in a hash table in memory. A billion 8 byte keys currently takes around 64GB of memory. 
Storing this in the Java Heap is a non-starter for a performance critical storage engine. HaloDB solves this problem by storing 
all data in native memory, outside the heap. Each key in the cache takes an additional 29 bytes for the metadata. Also, each bucket
in the hash table requires 8 bytes, and the total number of such buckets depends on the number of keys.  
Memory fragmentation is an always an issue with native memory allocation. **Using [jemalloc](http://jemalloc.net/) is highly recommended** as it provides 
a significant reduction in the cache's memory footprint and fragmentation.

For fixed size keys it is possible to further reduce the memory footprint and fragmentation by using a memory pool. Work on this 
is in progress and should be ready soon.   

### Delete operations.
Delete operation for a key will add a tombstone record to a tombstone file, which is distinct from the data files. 
This design has the advantage that the tombstone record once written need not be copied again during compaction, but 
the drawback is that in case of a power loss HaloDB cannot guarantee total ordering when put and delete operations are 
interleaved (although partial ordering for both is guaranteed).   

# Benchmarks.
  Benchmarks were run to compare HaloDB against RocksDB and KyotoCabinet (which is what HaloDB was written to replace) 
  All benchmarks were run on bare-metal box with the following specifications:
  * 2 x Xeon E5-2680 2.50GHz (HT enabled, 24 cores, 48 threads) 
  * 128 GB of RAM.
  * 1 Samsung PM863 960 GB SSD with XFS file system. 
  * RHEL 6 with kernel 2.6.32.  
  
  
  Key size was 8 bytes and value size 1024 bytes. Tests created a db with 500 million records with total size of approximately 
  500GB. Since this is significantly bigger than the available memory it will ensure that the workload will be IO bound, which is what HaloDB was primarily designed for.  
  
## Test 1: Fill Sequential.
Create a new db by inserting 500 million records in sorted key order.
![HaloDB](https://lh3.googleusercontent.com/P72df6yc2SONW3eogrRNZ3F4y39KyPdoHztDRvMroS5kXUDMTIH58aGb9Gysjx6GF3RJY-EbOMGE5hgJ06WvNuGLmSaHArpe24x8OiOnnsAsNRKRTN7m2JZJCZfD_jzEj8VHMZcyIxYDdOVSA95wB9YC3kxiDQTd0FiFOuu570IxaXZYwUAPtFlDlBURSJ_olbGl-hxjw0EY-OyM2yPhLXKZuJ_vsw4SJ6JFqAshUE9fgN4gcgKkHHYUgSVY6MBe8pM1sUJ96dW4uQ0EXH4n3D-cRciUEZAb2Uv6RJECZlCaDi66-Leix9UESopgOVYHhj9sb2fCjqy9puf2e3tufTwPavw7Rl4kQ1m_Q9apwkUnrciq3lzT3o-ajeGP3jzyw9v3PK1VA1rX29f-3PBwFIipP2ZY8JYnz0ETKGqVr6xchKd-AekSw0zxNsMu_LZRNyA5J3YPbCQr8yP4eoJH1HX_SY7sli4_dqCaXbvaFCOywORQTbF6YYtCQeq1MGwv2FShwJIXp-UMEg-2GrmAWm9EVsHXNZ6XXeUKh5aK_CTspMQeAw3w32Vo4py1pVDRWWevTCR30DEw70Mju9-IkkAxi_n_bT8paBo9mISkixxncXUPOd_lDTe_txC9uPJGtAU_Axl9Jyh-zMPpb6zJdZJw-fuf0kWw=w1790-h966-no)

DB size at the end of the test run. 

| Storage Engine    |    GB     |
| -------------     | --------- |
| HaloDB            | 503       |
| KyotoCabinet      | 609       |
| RocksDB           | 487       |


## Test 2: Random Read
Measure random read performance with 32 threads doing _640 million reads_ in total. Read ahead was disabled for this test.    
![HaloDB](https://lh3.googleusercontent.com/yCIQefIM3cLZ16ZcCQrood-xycLt_rISqmLroMwDnDNIpHj2pYQnOlEwg4DjpuOiTqaSz-le_sJmZGqvGGk-RjIyxkqXbfL0HbzEGWPrqDVCjw_jLj3bGbx2g_VvIy6H2YF2wL6GL1Z1YK5c5Flmuq6Br1yOi_pLDcfpF956vMQEveI5kti4l3ek7l8jLLjln4nXorIgaGIrJXZwxDFebn32TW1nDeGWz4uDjHVDScDMCIme_GUrTwVgjeF6H7IFW82YXn7ecVVd_5FTFesHbubBB48aqOCFkjQQOgwS05zHZsF4hO06fvbiDs2fex8tGddMYSO38VdKp-Rc6882KjZAhqcW20v8OLZOLBg07bc_9VcsKVhWqbvEVDOGzdHgIto7KG2KHHlKV0-XqJakjmVLwdQKOFE2RGf41czMSVWsCvIygOnkCBcoU34K1L2TPxcjhlk-EWyY9dnnFuBtc8UZl5yg3N-3t49Zd9mDfoLCgIazizKmOqxxxd3obcUAeSpMV2ibkaQKO0kAnX7VNSqR1OrsAzS1n8zWF2CGSurmYkaUJ3_WNPCdzyG2IaLFnC7cNhVR5j8wd_ziPRuDSv11-kx67r7o_6HEJY2QVzBbh5WbO53t0ufu1JGB8VoWcgtDKRiwqyCTxwKe18QawfcWJ8bs_Lyo=w1952-h1076-no)
   
## Test 3: Random Update.
Perform 500 million updates to randomly selected records.    
![HaloDB](https://lh3.googleusercontent.com/E4eGjSN-c-lKZ_13kInENupW5MLTUs4ZnMl5y8ZA7gXm7GI-oTC0pLXJZJH_u9Ip1FxpKcVNNN8NadWOrzMSQbEGVbQdlf-9kY07Qoy_ZfOBSLptA_Ym14GD3A2D0ySqH6dlkG3f3FmZv53Tlnx_jxE_ZY0uvqJcMhrS4Zk8RmKSL49RRRTsKe0WX3Ldeqkn9YGf67CHNjzQDHog6sprZdfCGWti1PjfCKytjV5JQ1iB3YLPdJJsyZIXA8oFUSV1qqfjPxXngheKlrN3Py1LN-e_Igj_HrqyTw6NpQLE9i_FLEQjeLdPkXmSHTf2OX402YgcnuqhB5AtBEDMX8OhuRxXPX6_mTySfrQrTkkfAa3JgB48Hac1uMxeyqS10YmX1g_0-brHqcK7wfeemhB4zy2SbKqka3I5piakVTTku4CUCsLeyI_8lmTIAaISVjOrUwQQpiDzTl4dtVD91oTo-hMvVfAgTiDtGUfN6f5yt4mm9F_RZ99o3IIBxWVMApZzMOso2IAMczIoHQtH7GeJmoDhhqVVBwrO4zfSAYPDeSNB0L1ZdvtIBnVtTrMQZgK9XYfQNKFALnXWusKBk6962k2Nd5JAWDHgsCsvUITxqOjnFb9-q3F4rAwcE_S_C0cmJNYgklK1bnPeGwqpJaDpEOrEWmTtcZn_=w1940-h1060-no)

DB size at the end of the test run. 

| Storage Engine    |    GB     |
| -------------     | --------- |
| HaloDB            | 556       |
| KyotoCabinet      | 609       |
| RocksDB           | 504       |

## Test 4: Fill Random. 
Insert 500 million records into an empty db in random order. 
![HaloDB](https://lh3.googleusercontent.com/6reqAGgw1S2Q2BJP9A-HBHs6CBvkRcxpX78tDLYi1usZpI1o1EsGjDQoUgnE-H33oV8SzylwLZSaFWafXpS-s4JzyheKK1dyOoO1xcacN0xI0jYAbNfZy5A_tnpLmvhM9FpOI0xOjGgvVe3jEZn5rYP9r2dxzVuQ4aK_mkJKJLc6ktTiNFmctH2B8wwq31bM-W5NAxNx8AaMfrQdsWmcK62LU-_-r5oXTaprkAN4pHo_jjhJ3pOkmk8u8llK9yIyxJjSDxNrVrUWdY_rYgRpYWXtJqFExAv9befCOM4_JWBEVKcTA9vmZdLcy2d1w1ia0_whIH-s4dTGO6FKp1TlF9s-HFuqj-P-mHcVGwPzJx_ARnsMNri6ts4dJAxahkUEBihqGTkohxSO9UQZ5D2s-IpRvsuAu1UGFjK0nUCiBNgT3SERXdUosj0Pd4pcBKNOpKfAl6338nigKYsIZXWCOb55mxUG3-ZbqXdCMCLMefrqg8BoxR8dtF5TssbTKsBTVPiM-XzsqeEaYsHuKIT3qnKFX3cXf2N9mCtjobdc9gsv2FalXRrA1hkCZIUDmr31yqUkv6Lo8jckap_uEGfM22cSewe2pDJOelEO67FX9tX-u7YMZ8JtyHllhYBFdP8rIHQNuByKu7RvvBiF5huUKo_gq-S-wv1c=w1944-h1062-no)

## Test 4: Read and update. 
32 threads doing a total of 640 million random reads and one thread doing random updates as fast as possible.  
![HaloDB](https://lh3.googleusercontent.com/sJtr8EdXWyw6IjG9oUn6Vb4YAW-KDnfMcqYTDAYOLO3N3sxt-FM-4JaA8hHKQeA63yzHZ9wGvxtp9BXDu-moxJ5K-bqFY2XBXUu4J82TiQ6SFOwC5UI73BxKdg05iS7dzJfe-lQM491xi_7aHnEfkZXOyxy0c8-zz_v4LgbeWILxGKHaGLyqj18dRIKpMw1Gv8fi5kvhSDu8YfsCp6BqZCI3CYUqduKnnHjFK7WAIvyaC6pFr3PkpU4C1ATpW9SGSeATlqbWOZgzMAVu7lZYJEi7xb3HMOkrc6w5kawVnJ62QBh9DRrila5F7fsEbR_sUPbL_WTYHvxMC0NVA2TjSUffg0wo4VJ75251s75DSLuB-Y3jlZ9i9vM6SCvGoPfeizgf8TU8iIc-9Ws9v4nLqewufM8ft4vlyoIA6aqUB_NVOtN7_FXJ40irUoEDzKDUP-cVzWlFWIpP1HXasxmbzwP34S1_oiyn2pAcC3VpGZ5RuzF-vjapscRdKYiFOJE8S5ywiZZYcCvOxwS3lKpMNs4Y_qkgPen3PTDALteoLyV9EKm90EJEMNw6Pm_amM_wj0pk7qjPpTlkhcSspwPXPvnWLJR2EhldWSFq32R8fUsFuFX5dRXmy4ORpHScuCAu5KYx2dwQSCR0WLyDvX8rKPlhNha3nece=w1950-h1066-no)

  
  
  