# Benchmarks  
  
  Benchmarks were run to compare HaloDB against RocksDB and KyotoCabinet.
  KyotoCabinet was chosen as we were using it in production. RockDB was chosen as it is a well known storage engine
  with good documentation.   
  All benchmarks were run on bare-metal box with the following specifications:
  * 2 x Xeon E5-2680 2.50GHz (HT enabled, 24 cores, 48 threads) 
  * 128 GB of RAM.
  * 1 Samsung PM863 960 GB SSD with XFS file system. 
  * RHEL 6 with kernel 2.6.32.  
  
  
  Key size was 8 bytes and value size 1024 bytes. Tests created a db with 500 million records with total size of approximately 
  500GB. Since this is significantly bigger than the available memory it will ensure that the workload will be IO bound, which is what HaloDB was primarily designed for.
  
  Benchmark tool can be found [here](https://github.com/yahoo/HaloDB/tree/master/benchmarks)  
  
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

## Why HaloDB is fast.
HaloDB is not necessarily a better storage engine than RocksDB or KyotoCabinet. HaloDB was written for a specific type of workload, and therefore had
the advantage of optimizing for that workload, the trade-offs that HaloDB makes might make it sub-optimal for certain other workloads. 
HaloDB also offers only a small subset of features that RocksDB supports.  
   
All writes to HaloDB are sequential writes to append-only log files. HaloDB uses a background compaction job to clean up stale data. 
The threshold at which a file is compacted can be tuned and this determines HaloDB's write amplification and space amplification. 
A compaction threshold of 50% gives a write amplification of only 2, this coupled with the fact that we do only sequential writes 
are the primary reasons for HaloDB’s high write throughput. Additionally, the only meta-data that HaloDB need to modify during writes are 
those of the index in memory. The trade-off here is that HaloDB will occupy more space on disk.    

To lookup the value for a key its corresponding metadata is first read from the in-memory index and then the value is read from disk. 
Therefore each lookup request requires at most a single read from disk, giving us a read amplification of 1, and is primarily responsible 
for HaloDB’s low read latencies. The trade-off here is that we need to store all the keys and their associated metadata in memory. HaloDB
also need to scan all the keys during startup to build the in-memory index. This, depending on the number of keys, might take time.   

HaloDB avoids doing in-place updates and doesn’t need record level locks, which also helps with performance even under high read and write throughput.

HaloDB also doesn't support range scans and hence doesn't pay the cost associated with storing data in a format suitable 
for efficient range scans.

