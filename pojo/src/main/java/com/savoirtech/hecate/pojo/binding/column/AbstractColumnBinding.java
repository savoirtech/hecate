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

package com.savoirtech.hecate.pojo.binding.column;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.exception.SchemaVerificationException;
import java.util.Optional;

public abstract class AbstractColumnBinding implements ColumnBinding {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected ColumnMetadata verifyColumn(TableMetadata tableMetadata, String name, DataType type) {
        Optional<ColumnMetadata> column = tableMetadata.getColumn(name);

        if (!column.isPresent()) {
            throw new SchemaVerificationException("Table \"%s\" does not contain column \"%s\" of type \"%s\".", tableMetadata.getName(), name, type.asCql(true, true));
        }
        if (!column.get().getType().equals(type)) {
            throw new SchemaVerificationException("Column \"%s\" in table \"%s\" is of the wrong type \"%s\" (expected \"%s\").", name, tableMetadata.getName(), column.get().getType().asCql(true, true), type.asCql(true, true));
        }
        return column.get();
    }

    protected void verifyPartitionKeyColumn(TableMetadata tableMetadata, String name, DataType type) {
        ColumnMetadata column = verifyColumn(tableMetadata, name, type);
        if(!tableMetadata.getPartitionKey().contains(column)) {
            throw new SchemaVerificationException("Column \"%s\" in table \"%s\" is not a partition key.", name, tableMetadata.getName());
        }
    }

    protected void verifyClusteringColumn(TableMetadata tableMetadata, String name, DataType type) {
        ColumnMetadata column = verifyColumn(tableMetadata, name, type);
        if(!tableMetadata.getClusteringColumns().containsKey(column)) {
            throw new SchemaVerificationException("Column \"%s\" in table \"%s\" is not a clustering column.", name, tableMetadata.getName());
        }
    }
}
