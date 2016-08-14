/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.dao.listener;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactoryBuilder;
import com.savoirtech.hecate.pojo.exception.SchemaVerificationException;
import com.savoirtech.hecate.test.Cassandra;
import com.savoirtech.hecate.test.CassandraRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.savoirtech.hecate.pojo.dao.listener.IdempotentCreateSchemaListener.CLAIM_ID_COL;
import static com.savoirtech.hecate.pojo.dao.listener.IdempotentCreateSchemaListener.DEFAULT_TABLE_NAME;
import static com.savoirtech.hecate.pojo.dao.listener.IdempotentCreateSchemaListener.KEY;

@Cassandra
public class IdempotentCreateSchemaListenerTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    @Rule
    public final CassandraRule cassandraRule = new CassandraRule();

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void createIdempotencyTable() {
        cassandraRule.getSession().execute(IdempotentCreateSchemaListener.createIdempotencyTable());
    }

    @Test
    public void testCreateSchema() {
        Session session = cassandraRule.getSession();
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactoryBuilder(session).withListener(new IdempotentCreateSchemaListener(session)).build();
        factory.createPojoDao(MyPojo.class);
        TableMetadata table = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable("my_pojo");
        assertNotNull(table);
    }

    @Test
    public void testCreateSchemaWhenAlreadyClaimed() {
        Session session = cassandraRule.getSession();
        session.execute(String.format("insert into %s (%s,%s) values ('%s', '%s')",
                DEFAULT_TABLE_NAME, KEY, CLAIM_ID_COL, "TABLE my_pojo", "12345"));

        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactoryBuilder(session).withListener(new IdempotentCreateSchemaListener(session)).build();
        factory.createPojoDao(MyPojo.class);
        TableMetadata table = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable("my_pojo");
        assertNull(table);
    }

    @Test(expected = SchemaVerificationException.class)
    public void testWhenIdempotencyTableNotFound() {
        Session session = cassandraRule.getSession();
        new IdempotentCreateSchemaListener(session,"fake_table");
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class MyPojo {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @PartitionKey
        private String id;
    }
}