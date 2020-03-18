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

package com.savoirtech.hecate.core.schema;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import java.util.LinkedList;
import java.util.List;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;

public class Table implements SchemaItem {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final String tableName;
    private final List<ColumnDefinition> partitionKeys = new LinkedList<>();
    private final List<ColumnDefinition> clusteringColumns = new LinkedList<>();
    private final List<ColumnDefinition> columns = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public Table(String tableName) {
        this.tableName = tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// SchemaItem Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public SimpleStatement createStatement() {
        CreateTableStart createStart = createTable(tableName).ifNotExists();

        CreateTable create = null;
        for (ColumnDefinition partitionKey : partitionKeys) {
            if (create == null) {
                create = createStart.withPartitionKey(partitionKey.getName(), partitionKey.getType());
            } else {
                create = create.withPartitionKey(partitionKey.getName(), partitionKey.getType());
            }
        }

        for (ColumnDefinition clusteringColumn : clusteringColumns) {
            create = create.withClusteringColumn(clusteringColumn.getName(), clusteringColumn.getType());
        }

        for (ColumnDefinition column : columns) {
            create = create.withColumn(column.getName(), column.getType());
        }

        return create.build();
    }

    @Override
    public String getKey() {
        return String.format("TABLE %s", tableName);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public Table addClusteringColumn(String name, DataType type) {
        clusteringColumns.add(new ColumnDefinition(name, type));
        return this;
    }

    public Table addColumn(String name, DataType type) {
        columns.add(new ColumnDefinition(name, type));
        return this;
    }

    public Table addPartitionKey(String name, DataType type) {
        partitionKeys.add(new ColumnDefinition(name, type));
        return this;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class ColumnDefinition {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final String name;
        private final DataType type;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public ColumnDefinition(String name, DataType type) {
            this.name = name;
            this.type = type;
        }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public String getName() {
            return name;
        }

        public DataType getType() {
            return type;
        }
    }
}
