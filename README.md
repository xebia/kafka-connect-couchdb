Kafka Connect CouchDB Connector
===============================
*This document is based on the 
[Kafka Connect Couchbase Connector](https://github.com/couchbase/kafka-connect-couchbase) README and modified 
for CouchDB and the Kafka Connect CouchDB Connector*

kafka-connect-couchdb is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect)
plugin for transferring data between CouchDB and Kafka.

It includes a "source connector" for publishing document change notifications from CouchDB to
a Kafka topic, as well as a "sink connector" that subscribes to one or more Kafka topics and writes the
messages to CouchDB.

This document describes how to configure and run the source and sink connectors.


## Set Up Kafka
If you already have an installation of Kafka and know how to start the servers, feel free to skip this 
section.

Setting up a basic installation is pretty easy. You can either download the archive from 
[Apache Kafka](https://kafka.apache.org/downloads) or install it with your preferred package manager.


### Installing from archive
Decompress the Apache Kafka archive and move the resulting directory under `~/opt` (or wherever you like to 
keep this kind of software). The rest of this guide refers to the root of the installation directory as 
`$KAFKA_HOME`.

Make sure the Kafka command line tools are in your path:

    export PATH=<path-to-apache-kafka>/bin:$PATH


### Start the Kafka Servers
Start the servers by running these commands, **each in a separate terminal**:

    zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
    kafka-server-start.sh $KAFKA_HOME/config/server.properties


# CouchDB Sink Connector
Now let's talk about the sink connector, which reads messages from one or more Kafka topics and writes them 
to CouchDB.

The sink connector will attempt to convert message values to JSON. If the conversion fails,
the connector will drop that specific message and log an error.

To decide whether to create a new document in CouchDB or update an existing one, the connector needs an unique 
identifier, hereafter referenced by ID. The ID is taken from the JSON object after parsing by using the value 
of a specific field of said object. Which field is used is decided by using a mapping from Kafka topic to 
field name. This allows for using a different ID field per Kafka topic.

The mapping is provided by setting the value for the `topics-to-id-fields-mapping` configuration key in 
`$KAFKA_CONNECT_COUCHDB_HOME/config/couchdb-sink.properties`.

You will need to specify a similar mapping from Kafka topic to CouchDB database. This will make sure messages 
from a specific Kafka topic will be stored in a specific CouchDB database. This databases will need to be 
present beforehand as the connector does not create them.

This mapping is provided by setting the value for the `topics-to-databases-mapping` configuration key in 
`$KAFKA_CONNECT_COUCHDB_HOME/config/couchdb-sink.properties`.


## Configure and Run the Sink Connector
Fill in all the remaining configuration fields in `couchdb-sink.properties` according to their descriptions.

Kafka connectors can be run in 
[standalone or distributed](https://kafka.apache.org/documentation/#connect_running) mode. For now let's run 
the connector in standalone mode, using the CLASSPATH environment variable to include the CouchDB connector 
JAR in the class path.

    cd $KAFKA_CONNECT_COUCHDB_HOME
    env CLASSPATH=./* \
        sh $KAFKA_HOME/connect-standalone.sh $KAFKA_CONFIG_HOME/connect-standalone.properties \
                           config/couchdb-sink.properties


### Alternatively, Run the Connector with Class Loader Isolation

Apache Kafka version 0.11.0 introduced a mechanism for plugin class path isolation. To take advantage of this 
feature, edit the connect worker config file (the `connect-*.properties` file in the above commands). Modify 
the `plugin.path` property to include the parent directory of `kafka-connect-couchdb-<version>.jar`.

Run the connector using the same commands as above, but omitting the `env CLASSPATH=./*` prefix. Each Kafka 
Connect plugin will use a separate class loader, removing the possibility of dependency conflicts.


## Send Test Messages
Now that the CouchDB Sink Connector is running, let's give it some messages to import:

    cd $KAFKA_CONNECT_COUCHDB_HOME/examples/json-producer
    mvn compile exec:java

The producer will send some messages and then terminate. If all goes well,
the messages will appear in the CouchDB database you specified in the sink connector config.

If you wish to see how the CouchDB Sink Connector behaves when the id field is set to be the airport code,
edit `couchdb-sink.properties` and set `topics-to-id-fields-mapping/airport`,
restart the sink connector, and run the producer again.
