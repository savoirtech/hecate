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

package com.savoirtech.hecate.pojo.binding.facet;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.Collection;
import java.util.function.Predicate;

import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.core.schema.Table;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.ElementBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.binding.column.SingleColumnBinding;
import com.savoirtech.hecate.pojo.facet.Facet;

public abstract class OneToManyFacetBinding<C,F> extends SingleColumnBinding<C,F> implements ColumnBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ElementBinding elementBinding;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public OneToManyFacetBinding(Facet facet, String columnName, ElementBinding elementBinding) {
        super(facet, columnName);
        this.elementBinding = elementBinding;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Collection<Object> elementsOf(F facetValue);

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public ElementBinding getElementBinding() {
        return elementBinding;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected void visitFacetChildren(F facetValue, Predicate<Facet> predicate, PojoVisitor visitor) {
        elementsOf(facetValue).forEach(element -> elementBinding.visitChild(element, predicate, visitor));
    }

    @Override
    public void describe(Table table, Schema schema) {
        super.describe(table, schema);
        elementBinding.describe(schema);
    }

    @Override
    public void verifySchema(KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata) {
        super.verifySchema(keyspaceMetadata, tableMetadata);
        elementBinding.verifySchema(keyspaceMetadata);
    }
}
