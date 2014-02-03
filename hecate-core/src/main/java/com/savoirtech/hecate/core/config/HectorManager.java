/*
 * Copyright 2014 Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.core.config;

import java.util.ArrayList;

import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.commons.lang.StringUtils;

public class HectorManager {

    /**
     * Gets the keyspace.
     *
     * @param clusterName          the cluster name
     * @param keyspaceConfigurator
     * @param columnFamilyName     the column family name   @return the keyspace
     */
    public Keyspace getKeyspace(final String clusterName, final CassandraKeyspaceConfigurator keyspaceConfigurator, final String columnFamilyName,
                                boolean isSuperColumn, String comparatorAlias) {

        Keyspace ks = null;

        try {

            ks = createKeyspace(clusterName, keyspaceConfigurator, columnFamilyName, isSuperColumn, comparatorAlias);
        } catch (HInvalidRequestException e) {
            // ignore it, it means keyspace already exists, but CF could not
            if (e.getWhy().startsWith("Keyspace names must be case-insensitively unique")) {
                try {

                    ks = createCF(clusterName, keyspaceConfigurator, columnFamilyName, isSuperColumn, comparatorAlias);
                } catch (HInvalidRequestException e1) {
                    // ignore it, it means keyspace & CF already exist, get the
                    // ks to hector client
                    if (e1.getWhy().startsWith("Cannot add already existing column family")) {

                        ks = HFactory.createKeyspace(keyspaceConfigurator.getKeyspace(), getOrCreateCluster(clusterName, keyspaceConfigurator),
                            keyspaceConfigurator.getConsistencyLevelPolicy(), keyspaceConfigurator.getFailoverPolicy(),
                            keyspaceConfigurator.getCredentials());
                    } else {
                        throw e1;
                    }
                }
            }
        }

        return ks;
    }

    /**
     * Creates the cf.
     *
     * @param clusterName
     */
    private Keyspace createCF(final String clusterName, final CassandraKeyspaceConfigurator keyspaceConfigurator, final String columnFamilyName,
                              boolean isSuperColumn, String comparatorAlias) {

        Cluster cluster = getOrCreateCluster(clusterName, keyspaceConfigurator);

        createCF(keyspaceConfigurator.getKeyspace(), columnFamilyName, cluster, isSuperColumn, comparatorAlias);

        Keyspace keyspace = HFactory.createKeyspace(keyspaceConfigurator.getKeyspace(), getOrCreateCluster(clusterName, keyspaceConfigurator),
            keyspaceConfigurator.getConsistencyLevelPolicy(), keyspaceConfigurator.getFailoverPolicy(), keyspaceConfigurator.getCredentials());

        return keyspace;
    }

    /**
     * Creates the keyspace.
     */
    private Keyspace createKeyspace(final String clusterName, final CassandraKeyspaceConfigurator keyspaceConfigurator, final String columnFamilyName,
                                    boolean isSuperColumn, String comparatorAlias) {
        Cluster cluster = getOrCreateCluster(clusterName, keyspaceConfigurator);

        KeyspaceDefinition ksDefinition = new ThriftKsDef(keyspaceConfigurator.getKeyspace());
        Keyspace keyspace = HFactory.createKeyspace(keyspaceConfigurator.getKeyspace(), cluster, keyspaceConfigurator.getConsistencyLevelPolicy(),
            keyspaceConfigurator.getFailoverPolicy(), keyspaceConfigurator.getCredentials());
        cluster.addKeyspace(ksDefinition);

        createCF(keyspaceConfigurator.getKeyspace(), columnFamilyName, cluster, isSuperColumn, comparatorAlias);
        return keyspace;
    }

    private void createCF(final String kspace, final String columnFamilyName, final Cluster cluster, boolean isSuperColumn, String comparatorAlias) {

        if (isSuperColumn) {
            ThriftCfDef cfDefinition = (ThriftCfDef) HFactory.createColumnFamilyDefinition(kspace, columnFamilyName, ComparatorType.UTF8TYPE,
                new ArrayList<ColumnDefinition>());
            cfDefinition.setColumnType(ColumnType.SUPER);
            cfDefinition.setSubComparatorType(ComparatorType.UTF8TYPE);
            cluster.addColumnFamily(cfDefinition, true);
        } else {
            ColumnFamilyDefinition familyDefinition = new ThriftCfDef(kspace, columnFamilyName, ComparatorType.UTF8TYPE);
            familyDefinition.setColumnType(ColumnType.STANDARD);
            //familyDefinition.setDefaultValidationClass("UTF8Type");

            if (!StringUtils.isEmpty(comparatorAlias)) {
                familyDefinition.setComparatorType(ComparatorType.COMPOSITETYPE);
                familyDefinition.setComparatorTypeAlias(comparatorAlias);
            }
            cluster.addColumnFamily(familyDefinition, true);
        }
    }

    /**
     * Gets the or create cluster.
     *
     * @param clusterName          the cluster name
     * @param keyspaceConfigurator
     * @return the or create cluster
     */
    public static Cluster getOrCreateCluster(final String clusterName, final CassandraKeyspaceConfigurator keyspaceConfigurator) {
        return HFactory.getOrCreateCluster(clusterName, keyspaceConfigurator.getHostConfigurator(), keyspaceConfigurator.getCredentials());
    }
}
