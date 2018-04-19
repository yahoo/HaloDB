# HaloDB

HaloDB is a fast, embedded, persistent key-value storage engine written in Java. 

HaloDB was primarily developed as an embedded storage engine for a high-throughput, low latency 
key-value database that powers Yahoo's DSP, and hence all its design choices and optimizations were
for this specific use case. 

HaloDB comprises of two main components: a cache in memory which stores all the keys, and files on
the persistent layer which stores all the data. To reduce Java garbage collection pressure the cache 
is allocated in native memory, outside the Java heap.

https://docs.google.com/presentation/d/1MckR3WnRgrCO5ekf7n-RmOpbuF6OeqHD3dAIXO4Y8Aw/edit?usp=sharing 

//TODO: more documentation in the works. 

#