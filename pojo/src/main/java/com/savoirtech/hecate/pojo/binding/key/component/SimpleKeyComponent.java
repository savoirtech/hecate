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

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.column.SimpleColumnBinding;
import com.savoirtech.hecate.pojo.binding.facet.SimpleFacetBinding;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

public abstract class SimpleKeyComponent extends SimpleColumnBinding implements KeyComponent {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SimpleKeyComponent(Facet facet, String columnName, Converter converter) {
        super(facet, columnName, converter);
    }

//----------------------------------------------------------------------------------------------------------------------
// KeyComponent Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public ColumnBinding createReferenceBinding(Facet referencingFacet, NamingStrategy strategy) {
        SubFacet sub = new SubFacet(referencingFacet, getFacet(), false);
        return new SimpleFacetBinding(sub, strategy.getColumnName(sub), getConverter());
    }

    @Override
    public Delete delete(OngoingWhereClause<Delete> delete) {
        return delete.whereColumn(getColumnName()).isEqualTo(bindMarker());
    }
}
