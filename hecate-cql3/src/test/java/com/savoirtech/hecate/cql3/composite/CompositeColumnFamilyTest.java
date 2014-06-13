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

import java.util.Iterator;

import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.cql3.persistence.PojoQuery;
import com.savoirtech.hecate.cql3.persistence.PojoQueryResult;
import com.savoirtech.hecate.cql3.persistence.def.DefaultPersistenceContext;
import com.savoirtech.hecate.cql3.test.CassandraTestCase;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompositeColumnFamilyTest extends CassandraTestCase {
    //----------------------------------------------------------------------------------------------------------------------
    // Fields
    //----------------------------------------------------------------------------------------------------------------------

    private final static String TABLE =
        "CREATE TABLE composite (\n" + "id text,\n" + "a text,\n" + "b text,\n" + "c text,\n" + "data text,\n" + "PRIMARY KEY ((id), a)\n" + ");";

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
    public void tesCompositeTable() throws Exception {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, Composite> dao = factory.createPojoDao(Composite.class);
        Composite composite = new Composite();
        composite.setId("NAME");
        composite.setA("Johan");
        composite.setB("");
        composite.setC("C");
        composite.setData("DATA1");

        dao.save(composite);

        composite.setId("NAME");
        composite.setA("James");
        composite.setB("");
        composite.setC("C");
        composite.setData("DATA2");
        dao.save(composite);

        Composite found = dao.findByKey(composite.getId());
        assertNotNull(found);
        assertEquals("DATA2", found.getData());
        assertEquals(composite.getId(), found.getId());

        DefaultPersistenceContext context = new DefaultPersistenceContext(connect());

        PojoQuery query = context.find(Composite.class).eq("id", "NAME").build();
        PojoQueryResult result = query.execute();

        int c = 0;
        if (result != null) {
            Iterator res = result.iterate();
            while (res.hasNext()) {
                res.next();
                c++;
            }
        }

        assertTrue(c==2);

        //Delete a specific row.


        //context.delete(Composite.class,"composite").eq("id", "NAME").eq("a","Johan").execute();

    }
}