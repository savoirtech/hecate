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

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class Schema {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final List<SchemaItem> schemaItems = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public Stream<SchemaItem> items() {
        return schemaItems.stream();
    }

    public Table createTable(String tableName) {
        return withSchemaItem(new Table(tableName));
    }

    private <T extends SchemaItem> T withSchemaItem(T item) {
        schemaItems.add(item);
        return item;
    }
}
