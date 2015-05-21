/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.mapping;

import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Id;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.mapping.column.ColumnType;

public class FacetMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Facet facet;
    private final ColumnType columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FacetMapping(Facet facet, ColumnType columnType) {
        this.facet = facet;
        this.columnType = columnType;
    }

    public String toString() {
        return facet.getName() + "@" + facet.getColumnName();
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public ColumnType getColumnType() {
        return columnType;
    }

    public Facet getFacet() {
        return facet;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public boolean isClusteringColumn() {
        return facet.hasAnnotation(ClusteringColumn.class);
    }

    public boolean isPartitionKey() {
        return facet.hasAnnotation(Id.class) || facet.hasAnnotation(PartitionKey.class);
    }
}
