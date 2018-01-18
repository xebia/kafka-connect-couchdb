[![Build status](https://travis-ci.org/xebia/kafka-connect-couchdb.svg?branch=master "Build status")](https://travis-ci.org/xebia/kafka-connect-couchdb)
[![Test coverage](https://codecov.io/gh/xebia/kafka-connect-couchdb/branch/master/graph/badge.svg "Test coverage")](https://codecov.io/gh/xebia/kafka-connect-couchdb)

Kafka Connect CouchDB Connector
===============================
kafka-connect-couchdb is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect)
plugin for transferring data between CouchDB and Kafka.

The project consists of two parts, namely a sink connector and a source connector. The sink connector is used 
to store data from Kafka into CouchDB. The source connector is used to publish data from CouchDB to Kafka. 


# CouchDB Sink Connector

The sink connector stores data from Kafka into CouchDB. Described below are it's features and configuration.


## Listening for records from specific Kafka topics and insert their values into specific CouchDB databases

By providing a mapping from Kafka topic to CouchDB database name using the `sink-topics-to-databases-mapping` 
config value, you can specify which topics should be written into which database. Make sure you also add all 
topics under the `topics` config value, otherwise they will be ignored.

Every topic you specify under `sink-topics-to-databases-mapping` should also be specified under 
`topics-to-id-fields-mapping` so that the connector knows what field to use to identify unique documents.


## Handling conflicts when they occur

CouchDB uses a conflict mechanism to deal with concurrent updates. When one of these conflicts occur the sink 
connector will resolve it. You have control over how this is done by writing your own implementation of 
`com.xebia.kafka.connect.couchdb.merging.Merger`, adding it to the classpath and providing it's complete path 
using the `merger` config value.

By default the `com.xebia.kafka.connect.couchdb.merging.LatestWinsMerger` is used. As the name implies it 
simply uses the latest (e.g. incoming) document as the new leading document and deletes the rest.

During conflict resolution the data might change and the process will have to start over. How many times this 
process is tried can be configured using the `max-conflicting-docs-fetch-retries` config value.


# CouchDB Source Connector

The source connector publishes data from CouchDB to Kafka. Described below are it's features and 
configuration.


## Publishing changes from specific CouchDB databases to specific Kafka topics

By providing a mapping from Kafka topic to CouchDB database name using the 
`source-topics-to-databases-mapping` config value, you can specify which CouchDB databases should be published 
to which Kafka topic. This is done by listening to the changes feed of the provided databases.
 
Specific metadata is added to each produced Kafka record, namely:
- The originating database (as source partition)
- The document revision (as offset)
- The document _id (as key)

As the CouchDB revisions are used as Kafka offsets, Kafka will know not to send old revisions to consumers who 
have already received them. Take note, however, that these documents will still be sent to Kafka and will 
take up memory if Kafka is configured to persist for a certain amount of time.
 
How far back the changes feed should be consulted can be configured per database using the 
`databases-to-changes-since-mapping` config value. The default is `0`, meaning all changes that have ever 
occurred in that specific database will be published. A value of `now` specifies all changes from the moment 
the connector starts listening. A specific update sequence ID can also be supplied so that all changes since 
that update sequence will be published.


## Batching changes

The changes from the CouchDB databases' changes feeds are streamed into the connector. Kafka itself, however,
 polls these changes. Therefore the changes are collected in memory until Kafka polls them. At that moment all 
changes in memory are returned until there are no more or the amount in the config value 
`max-source-batch-size` is reached. 

If there are a lot of changes coming in it might take a long time for the 
memory to be depleted, if it happens at all. Therefore the max batch size will ensure that once that size is 
reached the batch is given to Kafka for processing.
