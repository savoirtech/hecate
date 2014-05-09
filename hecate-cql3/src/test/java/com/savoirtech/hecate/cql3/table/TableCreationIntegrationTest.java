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

package com.savoirtech.hecate.cql3.table;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.HecateException;
import com.savoirtech.hecate.cql3.entities.CompoundKeyTable;
import com.savoirtech.hecate.cql3.entities.SimpleTable;
import com.savoirtech.hecate.cql3.farsandra.Farsandra;
import com.savoirtech.hecate.cql3.farsandra.LineHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertTrue;

public class TableCreationIntegrationTest {
    protected Cluster cluster;
    Farsandra fs;

    @Before
    public void setup() {
        fs = new Farsandra();
    }

    @After
    public void close() {
        if (fs != null) {
            try {
                fs.getManager().destroyAndWaitForShutdown(6);
            } catch (InterruptedException e) {

                e.printStackTrace();
            }
        }
    }

    @Test
    public void testShutdownWithLatch() throws InterruptedException, HecateException {
        fs.withVersion("2.0.7");
        fs.withCleanInstanceOnStart(true);
        fs.withInstanceName("3_1");
        fs.withCreateConfigurationFiles(true);
        fs.withHost("127.0.0.1");
        fs.withSeeds(Arrays.asList("127.0.0.1"));

        fs.withJmxPort(9999);
        fs.appendLineToYaml("start_native_transport: true");

        final CountDownLatch started = new CountDownLatch(1);
        fs.getManager().addOutLineHandler(new LineHandler() {
                                              @Override
                                              public void handleLine(String line) {
                                                  System.out.println("out " + line);
                                                  if (line.contains("Listening for thrift clients...")) {
                                                      started.countDown();
                                                  }
                                              }
                                          }
                                         );
        fs.start();
        started.await(10, TimeUnit.SECONDS);

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }

        Session session = cluster.connect();

        ResultSet resultSet = null;

        System.out.println("Create statement " + TableCreator.createTable("hecate", "simpletable", SimpleTable.class));

        session.execute("CREATE KEYSPACE hecate WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");
        try {
            resultSet = session.execute(TableCreator.createTable("hecate", "simpletable", SimpleTable.class));
        } catch (HecateException e) {
            e.printStackTrace();
        }

        assertTrue(resultSet != null);

        System.out.println("Create statement " + TableCreator.createTable("hecate", "compoundtable", CompoundKeyTable.class));
        try {
            resultSet = session.execute(TableCreator.createTable("hecate", "compoundtable", CompoundKeyTable.class));
        } catch (HecateException e) {
            e.printStackTrace();
        }

        assertTrue(resultSet != null);
    }
}
