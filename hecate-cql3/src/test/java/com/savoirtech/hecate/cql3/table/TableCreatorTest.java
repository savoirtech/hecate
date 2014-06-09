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

import com.savoirtech.hecate.cql3.entities.CompoundKeyTable;
import com.savoirtech.hecate.cql3.entities.ConflictKeyTable;
import com.savoirtech.hecate.cql3.entities.SimpleTable;
import com.savoirtech.hecate.cql3.exception.HecateException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TableCreatorTest {

    @Test
    public void testCreateTable() throws HecateException {

        String create = TableCreator.createTable("keySpace", "tableName", SimpleTable.class);
        System.out.println(create);
        assertTrue(create.equals(
                "CREATE TABLE IF NOT EXISTS keySpace.tableName ( id BIGINT PRIMARY KEY,name TEXT,more TEXT,date TIMESTAMP,aBoolean BOOLEAN,"
                        + "aDouble DOUBLE,aFloat FLOAT,uuid UUID );"
        ));

        create = TableCreator.createTable("keySpace", "tableName", CompoundKeyTable.class);
        System.out.println(create);
        assertTrue(create.equals("CREATE TABLE IF NOT EXISTS keySpace.tableName ( id BIGINT,name TEXT,more TEXT, PRIMARY KEY (id,name) );"));
    }

    @Test

    public void testConflictingKey() {
        String create = null;
        try {
            TableCreator.createTable("keySpace", "tableName", ConflictKeyTable.class);
        }
        catch (HecateException h) {
            h.printStackTrace();
        }
        assertTrue(create == null);
    }
}
