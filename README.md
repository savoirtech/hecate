Hecate
======

Hecate is a general-purpose utilities library for [Apache Cassandra] (http://cassandra.apache.org/).

Hecate was born from 2012 [Apache](http://www.apache.org/licenses/LICENSE-2.0)-licensed effort, originally created by
Jeff Genender.

At JavaOne 2013, Jeff Geneder and Johan Edstrom presented 
[Building a Country on Java Open Source](https://www.youtube.com/watch?v=hMGfEwLwMUc) where they describe Hecate's 
initial usage.

## The Early Days

Originally, Hecate was written using [Hector](http://hector-client.github.io/hector/build/html/index.html).  Hector's 
API was quite cumbersome for the developer, so Hecate was created to alleviate that burden by providing a simple-to-use 
API for storing Java objects in Cassandra.

## Enter CQL

In 2014, it became quite obvious that the future of Cassandra hinged upon the Cassandra Query Language (CQL).  Hecate
was completely rewritten using the [Java Driver for Apache Cassandra](https://github.com/datastax/java-driver).


