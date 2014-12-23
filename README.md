SiddiExtension-MaxK
===================

Max-K Transformer extension to siddi - Holds the Max K values of a stream for a temporary time period.

SetUp:
1. Compile that class, and add the jar to the class path <CEP_HOME>/repository/components/lib.
2. hen add the fully-qualified class name for the implementation class in a new line, 
   to the siddhi.extension file located at <CEP_HOME>/repository/conf/siddhi.
   
   
This is how to use the extension in a query:
  from inputStream#transform.debs:getVelocities(value, date, K, time) 
  select *
  insert into outStream;

NOTE : copy the source to <CEP_HOME>/samples/producers/ and build the extension.
