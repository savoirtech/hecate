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

package com.savoirtech.hecate.pojo.binding.key.component;

import java.util.List;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Create;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;

public class ClusteringColumnComponent extends SimpleKeyComponent {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ClusteringColumnComponent(Facet facet, String columnName, Converter converter) {
        super(facet, columnName, converter);
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void describe(Create create, List<Create> nested) {
        create.addClusteringColumn(getColumnName(), getDataType());
    }

    @Override
    public void verifySchema(KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata) {
        verifyClusteringColumn(tableMetadata, getColumnName(), getDataType());
    }

//----------------------------------------------------------------------------------------------------------------------
// KeyComponent Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public int getOrder() {
        return getFacet().getAnnotation(ClusteringColumn.class).order();
    }

    @Override
    public int getRank() {
        return 2;
    }
}
