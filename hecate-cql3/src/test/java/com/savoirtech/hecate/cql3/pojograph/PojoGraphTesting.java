/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.cql3.pojograph;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.dao.GenericTableDao;
import com.savoirtech.hecate.cql3.dao.abstracts.GenericPojoGraphDao;
import com.savoirtech.hecate.cql3.entities.Child;
import com.savoirtech.hecate.cql3.entities.Parent;
import com.savoirtech.hecate.cql3.exception.HecateException;
import com.savoirtech.hecate.cql3.table.TableCreator;
import com.savoirtech.hecate.cql3.test.CassandraTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class PojoGraphTesting extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testInsertData() throws InterruptedException, HecateException {
        Metadata metadata = getCluster().getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }

        Session session = getCluster().connect();

        ResultSet resultSet = null;
        Map<String, StringBuilder> tables = new HashMap<>();

        System.out.println("Create statement " + TableCreator.createNestedTables(tables, "hecate", "parent", Parent.class));

        for (Map.Entry<String, StringBuilder> entry : tables.entrySet()) {
            System.out.println(" " + entry.getKey() + " => " + entry.getValue().toString());
            resultSet = session.execute(entry.getValue().toString());
        }
        try {
            String create = TableCreator.createTable("hecate", "parent", Parent.class);

            resultSet = session.execute(create);
        }
        catch (HecateException e) {
            e.printStackTrace();
        }

        assertNotNull(resultSet);

        GenericTableDao dao = new GenericPojoGraphDao(session, "hecate", "parent", long.class, Parent.class);
        Parent pj = new Parent();
        pj.setId(100l);
        pj.setChild(new Child("V"));
        pj.getChildList().add(new Child("LIST"));
        pj.getChildList().add(new Child("LIST2"));
        pj.getChildSet().add(new Child("SET"));
        pj.getChildMap().put("A", new Child("MAP"));
        pj.getLongChildMap().put(1l, new Child("LONG"));

        dao.save(pj);
        Parent fC = (Parent) dao.find(100l);

        assertNotNull(fC);

        assertEquals(100l, fC.getId());
        assertEquals("MAP", fC.getChildMap().get("A").getName());

        assertEquals("V", fC.getChild().getName());

        System.out.println("The stored class returned " + fC);

        Parent emptyParent = new Parent();
        emptyParent.setId(200l);

        dao.save(emptyParent);
        Parent fempty = (Parent) dao.find(200l);

        assertNotNull(fempty);

        assertEquals(200l, fempty.getId());

        dao.delete(100l);

        fempty = (Parent) dao.find(100l);

        assertNull(fempty);
    }
}
