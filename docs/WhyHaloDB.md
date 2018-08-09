 
# HaloDB at Yahoo.

At Yahoo, we built this high throughput, low latency distributed key-value database that runs in multiple data centers in the US, Europe, and Asia-Pacific regions. 
The largest of these clusters stores around 80 billion records and handles, at peak, around 3 million read requests and 1 million write requests per second 
with an SLA of 1 millisecond at the 99th percentile.
 
The data we have in this database must be persistent, and the working set is larger than what we can fit in memory. 
Therefore, a key component of the database’s performance is a fast storage engine, for which we have relied on Kyoto Cabinet. Although Kyoto Cabinet has served us well, 
it was designed primarily for a read-heavy workload and its write throughput started to be a bottleneck as we took on more write traffic. 

 
There were also other issues we faced with Kyoto Cabinet; it takes hours to repair a corrupted db, or iterate over and update/delete records (which we have to do every night). 
It also doesn't expose enough operational metrics or logs which makes resolving issues challenging. However, our primary concern was Kyoto Cabinet’s write performance, 
which based on our projections, would have been a major obstacle for scaling the database; therefore, it was a good time to look for alternatives.
 
**These are the salient features of the database’s workload for which the storage engine will be used:**
* Small keys (8 bytes) and large values (10KB average)
* Both read and write throughput are high.
* Submillisecond read latency at the 99th percentile. 
* Single writer thread. 
* No need for ordered access or range scans.
* Working set is much larger than available memory, hence workload is IO bound.
* Database is written in Java.


## Why a new storage engine?
Although there are umpteen number of storage engines publicly available almost all use a variation of the following data structures to organize data on disk for fast lookup:
* __Hash table__: Kyoto Cabinet. 
* __Log-structured merge tree__: LevelDB, RocksDB.
* __B-Tree/B+ Tree__: Berkeley DB, InnoDB. 

Since our workload requires very high write throughput, Hash table and B-Tree based storage engines were not suitable as they need to do random writes. 
Although modern SSDs have narrowed the gap between sequential and random write performance, sequential writes still have higher throughput, primarily due 
to the reduced internal garbage collection load within the SSD. LSM trees also turned out to be unsuitable; benchmarking RocksDB on our workload showed 
a write amplification of 10-12, therefore writing 100MB/sec to RocksDB meant that it will write more than 1 GB/sec to the SSD, clearly too high. 
High write amplification of RocksDB is a property of the LSM data structure itself, thereby ruling out storage engines based on LSM trees. 

LSM tree and B-Tree also maintain an ordering of keys to support efficient range scans, but the cost they pay is a read amplification greater than 1, 
and for LSM tree, very high write amplification. Since our workload only does point lookups, we don’t want to pay the cost associated with storing data 
in a format suitable for range scans. 

These problems ruled out most of the publicly available and well maintained storage engines. Looking at alternate storage engine data structures led us to 
explore ideas used in Log-structured storage systems. Here was a potential good fit; log-structured system only does sequential writes, an efficient 
garbage collection implementation can keep write amplification low, and having an index in memory for the keys can give us a read amplification of one, 
and we get transactional updates, snapshots, and quick crash recovery almost for free. Also in this scheme, there is no ordering of data and hence its 
associated costs are not paid. We found that similar ideas have been used in [BitCask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf) 
and [Haystack](https://code.facebook.com/posts/685565858139515/needle-in-a-haystack-efficient-storage-of-billions-of-photos/). 
But BitCask was written in Erlang, and since our database runs on the JVM running Erlang VM on the same box and talking to it from the JVM is something 
that we didn’t want to do. Haystack, on the other hand, is a full-fledged distributed database optimized for storing photos, and its storage engine hasn’t been open sourced.  
Therefore it was decided to write a new storage engine from scratch; thus the HaloDB project was initiated. 

## Performance test results on our production workload. 
The following chart shows the results of performance tests that we did with real production data. The read requests were kept at 50,000 QPS while the write QPS was increased.

![SSD](https://lh3.googleusercontent.com/VKaFiNoM2zzyJ6TrGO6IHBcmT4pNUouGlhtYPQLwNfSyV2jvK1oBEvYevGaJ-AGYVHoM0M2VmOUx9U3KNmFXNyVbg6bHqwR-iAhAavtY6rF1JYFskCv4Vc8mLlGaS_LB2PthhpGwzRhB0FBGziIq5bvfTY-yuLKYbTgT8taWyeq1Dda6BvjSQ-jj1-d2IGixi6zADNOJ9XoVMQZxO6hGTECdzHgvZi7rqy95f2kGx1C4MIT6TwvEzavJztEBDZGS_fLNwnIHPVz5aNrzkC5GVSt80IelR4wllginxPsp0ja30dAc1bPFq4pjSHj-gWiXhqpAqTCTPmosqly8yuTQyV2QnXSI-X8TYSwgazsvgeMKmxnav7mTSA2mf1ljU1D34h0e_xiIRiQcTvEvhc_dvf9LKJWBfVEQdE4tfvfOHcfGotk868BO4zmsYcOOsWyQl4eg9gTMjdBBmcmnh8qwBIKGX3j0uc8zc6RITGcdFRFzh59sR3Gop0-cNk5HvKJlyzWSO0DQgDVzUeLrBj1FvV4zclAn3hoLmO8n51fKDy3lrctvhSxIH-wxSdy4hZWEQYGc8KdDHGpN4KCTwinEiqh5rsOIBhBc1JC9DFMgD7CI_gA1gvweVp25grC5AmxkuMMxG2nqZ2Kr99WLecC1QsDN0FP2CGmB=w1970-h1106-no) 
As you can see at the 99th percentile HaloDB read latency is an order of magnitude better than that of Kyoto Cabinet. 
We recently upgraded our SSDs to PCIe NVMe SSDs. This has given us a significant performance boost and has narrowed the gap between HaloDB and Kyoto Cabinet, 
but the difference is still significant:

![PCIe NVMe SSD](https://lh3.googleusercontent.com/S7q5hJtfhso5oT1_4fm8IJeFBxi6FDIZHhDQZw3664BAOz-DIkIoRdvE8pTCfjmtNGV61iU1YPtUctBMSvjFJuvmLUU7jCVVXaijCZlEWX7PZguQ-AIbou3NvRlkWcJjRvCi2bIei4mIchReUlBaB9WG8VChzifMfjNbYx6n7KwOBX64lDZM3XG5ACNIvuORLhgs4NVLdjbJd30_rA-luKapvjX0VSw8xTMxtY0i9HXDcdyDN0Wk6ikzUJI54r29tMoyeiG6bzblOGPJXbH_jmp28oplvxRs6FwexK2dy0cQlasOI87esweNF0H4cqLEWWYbgipooxhBqY9ZIpxSiEmwBIpUpOCYfNe41waF94ZngzdIUtiAgE40oTViDcjA1wUtwfLmRVDNYU9j3OWnRdbfYJ5Ha1bYbZx9pil9ntMzCZuj1dEmI5I1_DwINIYjSNBDMorZ-LynFwmkEOoUPqpGxMdSB2CexH5p7CRAXfd6pA3m2hY-cmg8NAQHKfW_krKSYyfTlN0cLBA9-CaWappMnqMSAiD176MMQnNy1QxIrsLQ_fkMYCcS-pi39XILZTISZccWvZa84u2suZ7ampZQ8tFD2vTPGLgonlpryo5RXci1iXQRXxuToWW8c9jxCkbjOorilHZqh84C3vX9DkXN9qBTYNM5=w1970-h1100-no)
 
Of course, these are results from performance tests, but nothing beats real data from hosts running in production.
Following chart shows the 99th percentile latency from a production server before and after migration to HaloDB.

![99th percentile in ms](https://lh3.googleusercontent.com/lg4VILxDcNwi2XOUCbJrFtKgTN7z08tLC6SpFyEyXqs4CDropEBAljQa5mf4LLG5fSOaXXKJs5HwEl15ID3x8JgVK7Acdz-tCyNVQouSdeOw6KAFZDN8L4_--ojDr3IxkI9rho7NEPuvm-yt80ZHF33jxKV5TlxsW7xXDxIe7OoOi9QkpwfID_QMTlJwfRBxOHf3G2PeHYRjc23UqO8Y1zDuDvRcqCF09oNM2w_K-3dpr6P5ihdOF5k054Nr4WXaNfdiMmjzKtvR8k5YGGTNSOunzVwc0YzI4TKKV3URTTjkEjhZzdl4DfHwjD8t507nPHpKh6OpUmlXcaUJkp7RHx5pGDTGTN86PMQxfNMizOm89NXc8ULivzCTZOdIMdX5BXtS0oza2N5ZgW9xRNcrj6GjY81AiiMfiHYVqAnRPXzhWTciwnzQ8AtZGihTjbnR6zKkrOQ00H1O-ZoeDcnWBPsP5KjAmXTyE3zXMXMRhZdc26yX9JGFctM7755h2NfK9rqXqz60sd6-0uaDTujipl7pLw_l7kAXTYiBHNwQsmE07HTVLWk-L_uA9Cnk_S017YWz64zP3bcnzOhuLAIOaTCzslBboPHX0g0WxR7MeLUgpj4mXpn6TUMKu95CmUFOa91HVwX-X3E1g2PXsESLznoZG_wE62kJ=w2092-h852-no) 
 
HaloDB has thus given our production boxes a 50% improvement in capacity while consistently maintaining a sub-millisecond latency at the 99th percentile.
 
HaloDB also has fixed few other problems that we had with KyotoCabinet. The daily cleanup job that used to take upto 5 hours in Kyoto Cabinet is now complete in 90 minutes 
with HaloDB due to its improved write throughput. Also, HaloDB takes only a few seconds to recover from a crash due to the fact that all log files, 
once they are rolled over, are immutable. Hence, in the event of a crash only the last file that was being written to need to be repaired. 
Whereas, with Kyoto Cabinet crash recovery used to take more than an hour to complete. And the statistics that HaloDB exposes gives us good insight into its internal state, 
which was missing with Kyoto Cabinet.
 
We invite you to checkout HaloDB. Try it with your use cases and let us know what you think. 
You can open issues on our github repo, and we'd welcome code contributions too. Let us know what you'd like to see added to HaloDB and what you plan on doing with it.
 
 
