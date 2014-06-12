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

package com.savoirtech.hecate.cql3.composite;

import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.cql3.test.CassandraTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CompositeColumnFamilyTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final static String TABLE =
            "CREATE TABLE composite (\n" + "id text,\n" + "a text,\n" + "b text,\n" + "c text,\n" + "data text,\n" + "PRIMARY KEY (id, a, b, c)\n" + ");";

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------------------------------------------
    // Other Methods
    //----------------------------------------------------------------------------------------------------------------------
    @Before
    public void createTable() {
        connect().execute(TABLE);
    }

    @Test
    public void testSave() throws Exception {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, Composite> dao = factory.createPojoDao(Composite.class);
        Composite composite = new Composite();
        composite.setId("NAME");
        composite.setA("Johan Edstrom");
        composite.setB("");
        composite.setC("C");
        composite.setData("DATA");
        dao.save(composite);

        composite.setId("NAME");
        composite.setA("James Carman");
        composite.setB("");
        composite.setC("C");
        composite.setData("DATA");
        dao.save(composite);

        Composite found = dao.findByKey(composite.getId());
        assertNotNull(found);
        assertEquals("DATA", found.getData());
        assertEquals(composite.getId(), found.getId());

        System.out.println(connect().execute("select * from composite where  id = 'NAME' and a > '';").all());
    }
}