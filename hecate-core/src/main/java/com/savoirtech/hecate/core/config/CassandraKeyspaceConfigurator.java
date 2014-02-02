/*
 * Copyright (c) 2012. Latinus S.A.
 */

package com.savoirtech.hecate.core.config;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;

public class CassandraKeyspaceConfigurator {

    private CassandraHostConfigurator hostConfigurator;
    private FailoverPolicy failoverPolicy;
    private ConsistencyLevelPolicy consistencyLevelPolicy;
    private String keyspace;
    private Map<String, String> credentials = new HashMap<String, String>();

    public CassandraKeyspaceConfigurator(CassandraHostConfigurator hostConfigurator, String keyspace, FailoverPolicy failoverPolicy,
                                         ConsistencyLevelPolicy consistencyLevelPolicy, Map<String, String> credentials) {
        this.hostConfigurator = hostConfigurator;
        this.keyspace = keyspace;
        this.failoverPolicy = failoverPolicy;
        this.consistencyLevelPolicy = consistencyLevelPolicy;
        this.credentials = credentials;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public CassandraHostConfigurator getHostConfigurator() {
        return hostConfigurator;
    }

    public void setHostConfigurator(CassandraHostConfigurator hostConfigurator) {
        this.hostConfigurator = hostConfigurator;
    }

    public FailoverPolicy getFailoverPolicy() {
        return failoverPolicy;
    }

    public void setFailoverPolicy(FailoverPolicy failoverPolicy) {
        this.failoverPolicy = failoverPolicy;
    }

    public ConsistencyLevelPolicy getConsistencyLevelPolicy() {
        return consistencyLevelPolicy;
    }

    public void setConsistencyLevelPolicy(ConsistencyLevelPolicy consistencyLevelPolicy) {
        this.consistencyLevelPolicy = consistencyLevelPolicy;
    }

    public Map<String, String> getCredentials() {
        return credentials;
    }

    public void setCredentials(Map<String, String> credentials) {
        this.credentials = credentials;
    }
}
