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

package com.savoirtech.hecate.cql3.meta;

import com.savoirtech.hecate.cql3.util.HecateUtils;
import com.savoirtech.hecate.cql3.value.Facet;
import org.apache.commons.lang3.Validate;

public class FacetMetadata {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Facet facet;
    private final String columnName;
    private final boolean identifier;
    private final String tableName;
    private final String indexName;
    private final boolean indexed;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FacetMetadata(Facet facet) {
        this.facet = Validate.notNull(facet, "Facet cannot be null.");
        this.columnName = HecateUtils.columnName(facet);
        this.identifier = HecateUtils.isIdentifier(facet);
        this.tableName = HecateUtils.tableName(facet);
        this.indexName = HecateUtils.indexName(facet);
        this.indexed = HecateUtils.isIndexed(facet);
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public String getColumnName() {
        return columnName;
    }

    public Facet getFacet() {
        return facet;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isIdentifier() {
        return identifier;
    }

    public boolean isIndexed() {
        return indexed;
    }
}
