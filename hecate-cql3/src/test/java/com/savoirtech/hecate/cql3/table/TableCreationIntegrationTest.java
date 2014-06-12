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

package com.savoirtech.hecate.cql3.table;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.entities.CompoundKeyTable;
import com.savoirtech.hecate.cql3.entities.SimpleTable;
import com.savoirtech.hecate.cql3.exception.HecateException;
import com.savoirtech.hecate.cql3.test.CassandraTestCase;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TableCreationIntegrationTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testWithCompoundTable() throws HecateException {
        assertTableCreateSucceeds(TableCreator.createTable("hecate", "compoundtable", CompoundKeyTable.class));
    }

    private void assertTableCreateSucceeds(String cql) throws HecateException {
        logger.info("Create statement {}", cql);

        Session session = connect();
        ResultSet resultSet = session.execute(TableCreator.createTable("hecate", "simpletable", SimpleTable.class));
        assertNotNull(resultSet);
    }

    @Test
    public void testWithSimpleTable() throws HecateException {
        assertTableCreateSucceeds(TableCreator.createTable(KEYSPACE_NAME, "simpletable", SimpleTable.class));
    }
}
