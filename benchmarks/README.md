# Storage Engine Benchmark Tool. 

Build the package using **mvn clean package** This will create a far jar *target/storage-engine-benchmark-1.0.jar*

Different benchmarks can be run using:
 
`java -jar storage-engine-benchmark-1.0-SNAPSHOT.jar <db directory> <benchmary type>`

Different benchmark types are defined [here](https://github.com/yahoo/HaloDB/blob/master/benchmarks/src/main/java/com/oath/halodb/benchmarks/Benchmarks.java). 