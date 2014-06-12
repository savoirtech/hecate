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

package com.savoirtech.hecate.cql3.mapping;

import com.savoirtech.hecate.cql3.handler.ColumnHandler;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;

public class FacetMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final FacetMetadata facet;
    private final ColumnHandler<Object, Object> columnHandler;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FacetMapping(FacetMetadata facet, ColumnHandler<Object, Object> columnHandler) {
        this.facet = facet;
        this.columnHandler = columnHandler;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public ColumnHandler<Object, Object> getColumnHandler() {
        return columnHandler;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(facet.getColumnName());
        sb.append(" ");
        sb.append(columnHandler.getColumnType());
        if (facet.isIdentifier()) {
            sb.append(" PRIMARY KEY");
        }
        return sb.toString();
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public FacetMetadata getFacetMetadata() {
        return facet;
    }
}
