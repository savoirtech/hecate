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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactoryBuilder;
import com.savoirtech.hecate.pojo.exception.SchemaVerificationException;
import com.savoirtech.hecate.test.CassandraSingleton;
import java.util.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.savoirtech.hecate.pojo.dao.listener.IdempotentCreateSchemaListener.CLAIM_ID_COL;
import static com.savoirtech.hecate.pojo.dao.listener.IdempotentCreateSchemaListener.DEFAULT_TABLE_NAME;
import static com.savoirtech.hecate.pojo.dao.listener.IdempotentCreateSchemaListener.KEY;

public class IdempotentCreateSchemaListenerTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void createIdempotencyTable() {
        CassandraSingleton.getSession().execute(IdempotentCreateSchemaListener.createIdempotencyTable());
    }

    @After
    public void after() {
        CassandraSingleton.clean();
    }

    @Test
    public void testCreateSchema() {
        CqlSession session = CassandraSingleton.getSession();
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactoryBuilder(session).withListener(new IdempotentCreateSchemaListener(session)).build();
        factory.createPojoDao(MyPojo.class);
        TableMetadata table = session.getMetadata().getKeyspace(session.getKeyspace().get()).get().getTable("my_pojo").get();
        assertNotNull(table);
    }

    @Test
    public void testCreateSchemaWhenAlreadyClaimed() {
        CqlSession session = CassandraSingleton.getSession();
        session.execute(String.format("insert into %s (%s,%s) values ('%s', '%s')",
                DEFAULT_TABLE_NAME, KEY, CLAIM_ID_COL, "TABLE my_pojo", "12345"));

        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactoryBuilder(session).withListener(new IdempotentCreateSchemaListener(session)).build();
        factory.createPojoDao(MyPojo.class);
        Optional<TableMetadata> table = session.getMetadata().getKeyspace(session.getKeyspace().get()).get().getTable("my_pojo");
        assertFalse(table.isPresent());
    }

    @Test(expected = SchemaVerificationException.class)
    public void testWhenIdempotencyTableNotFound() {
        CqlSession session = CassandraSingleton.getSession();
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