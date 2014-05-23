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

import com.savoirtech.hecate.cql3.HecateException;
import com.savoirtech.hecate.cql3.entities.CompoundKeyTable;
import com.savoirtech.hecate.cql3.entities.ConflictKeyTable;
import com.savoirtech.hecate.cql3.entities.SimpleTable;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TableCreatorTest {

    @Test
    public void testCreateTable() throws HecateException {

        String create = TableCreator.createTable("keySpace", "tableName", SimpleTable.class);
        assertEquals("CREATE TABLE IF NOT EXISTS keySpace.tableName (aboolean BOOLEAN, adouble DOUBLE, afloat FLOAT, " +
                "date TIMESTAMP, id BIGINT PRIMARY KEY, more TEXT, name TEXT, uuid UUID);", create);

        create = TableCreator.createTable("keySpace", "tableName", CompoundKeyTable.class);
        assertEquals("CREATE TABLE IF NOT EXISTS keySpace.tableName (id BIGINT, more TEXT, name TEXT, PRIMARY KEY (id,name));", create);
    }

    @Test

    public void testConflictingKey() {
        String create = null;
        try {
            create = TableCreator.createTable("keySpace", "tableName", ConflictKeyTable.class);
        } catch (HecateException h) {
            h.printStackTrace();
        }
        assertTrue(create == null);
    }
}
