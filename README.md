Hecate
======

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.savoirtech.hecate/hecate-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.savoirtech.hecate/hecate-parent)
[![Javadocs](https://javadoc.io/badge/com.savoirtech.hecate/hecate-parent.svg)](https://javadoc.io/doc/com.savoirtech.hecate/hecate-parent)
[![Build Status](https://travis-ci.org/savoirtech/hecate.svg?branch=master)](https://travis-ci.org/savoirtech/hecate)
[![Quality Gate](https://sonarqube.com/api/badges/gate?key=com.savoirtech.hecate:hecate-parent)](https://sonarqube.com/dashboard?id=com.savoirtech.hecate%hecate-parent)
[![Coverage](https://sonarqube.com/api/badges/measure?key=com.savoirtech.hecate:hecate-parent&metric=coverage)](https://sonarqube.com/dashboard?id=com.savoirtech.hecate%3Ahecate-parent)
[![Tech Debt](https://sonarqube.com/api/badges/measure?key=com.savoirtech.hecate:hecate-parent&metric=sqale_debt_ratio)](https://sonarqube.com/dashboard?id=com.savoirtech.hecate%3Ahecate-parent)
[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

Hecate is a general-purpose utilities library for [Apache Cassandra] (http://cassandra.apache.org/).

Hecate was born from a 2012 [Apache](./LICENSE.txt)-licensed effort, originally created by
Jeff Genender.

At JavaOne 2013, Jeff Geneder and Johan Edstrom presented 
[Building a Country on Java Open Source](https://www.youtube.com/watch?v=hMGfEwLwMUc) where they describe Hecate's 
initial usage.

## The Early Days

Originally, Hecate was written using [Hector](http://hector-client.github.io/hector/build/html/index.html).  Hector's 
API was quite cumbersome for the developer, so Hecate was created to alleviate that burden by providing a simple-to-use 
API for storing Java objects in Cassandra.

## Enter CQL

It became quite obvious that the future of Cassandra hinged upon the Cassandra Query Language (CQL).  In 2014, Hecate
was completely rewritten using the [Java Driver for Apache Cassandra](https://github.com/datastax/java-driver).

## The Magic

Hecate is a Greek goddess often associated with magic and witchcraft.  The majority of Hecate's (the library, 
not the goddess) magic resides in the [POJO](pojo/README.md) library.


