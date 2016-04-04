Hecate
======

Hecate came out of an ASF Licensed effort, originally created by Jeff Genender.

At JavaOne 2013, Jeff Geneder and Johan Edstrom presented 
[Building a Country on Java Open Source](https://www.youtube.com/watch?v=hMGfEwLwMUc) where they describe Hecate's 
initial usage.

======

This has become a library that we use frequently at http://savoirtech.com as we very rapidly can build
new data models, index, and search those.

It is a library that has undergone several iterations and heavy load testing.
In the simplest DAO solution we hold around 5000 req/s against an 8 node VM based cluster.
(We never got any further than that since the surrounding HW failed)

If you want more of the background on this - https://oracleus.activeevents.com/2013/connect/sessionDetail.ww?SESSION_ID=3674

Since then a CQL mapper and library has been added, still retaining a very simple
model for quickly building Java applictions without having to worry about Cassandra.



