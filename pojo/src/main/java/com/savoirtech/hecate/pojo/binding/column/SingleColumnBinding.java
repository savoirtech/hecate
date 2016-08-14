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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.core.schema.Table;
import com.savoirtech.hecate.pojo.binding.ParameterBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

public abstract class SingleColumnBinding<C, F> extends AbstractColumnBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleColumnBinding.class);

    private final Facet facet;
    private final String columnName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SingleColumnBinding(Facet facet, String columnName) {
        this.facet = facet;
        this.columnName = columnName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract DataType getDataType();

    protected abstract C toColumnValue(F facetValue);

    protected abstract F toFacetValue(C columnValue, PojoQueryContext context);

    protected abstract void visitFacetChildren(F facetValue, Predicate<Facet> predicate, PojoVisitor visitor);

//----------------------------------------------------------------------------------------------------------------------
// ColumnBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public void collectParameters(Object pojo, List<Object> parameters) {
        F facetValue = getFacetValue(pojo);
        if (facetValue == null) {
            parameters.add(nullColumnValue());
        } else {
            parameters.add(toColumnValue(facetValue));
        }
    }

    @Override
    public void describe(Table table, Schema schema) {
        table.addColumn(getColumnName(), getDataType());
    }

    @Override
    public List<ParameterBinding> getParameterBindings() {
        return Collections.singletonList(new SingleColumnParameterBinding());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void injectValues(Object pojo, Iterator<Object> columnValues, PojoQueryContext context) {
        C columnValue = (C) columnValues.next();
        if (columnValue == null) {
            getFacet().setValue(pojo, nullFacetValue());
        } else {
            getFacet().setValue(pojo, toFacetValue(columnValue, context));
        }
    }

    @Override
    public void insert(Insert insert) {
        insert.value(getColumnName(), bindMarker());
    }

    @Override
    public void select(Select.Selection select) {
        select.column(getColumnName());
    }

    @Override
    public void verifySchema(KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata) {
        verifyColumn(tableMetadata, getColumnName(), getDataType());
    }

    @Override
    public void visitChildren(Object pojo, Predicate<Facet> predicate, PojoVisitor visitor) {
        if(predicate.test(facet)) {
            F facetValue = getFacetValue(pojo);
            if(facetValue != null) {
                visitFacetChildren(facetValue, predicate, visitor);
            }
        }
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

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    protected F getFacetValue(Object pojo) {
        return (F) getFacet().getValue(pojo);
    }

    protected C nullColumnValue() {
        return null;
    }

    protected F nullFacetValue() {
        return null;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class SingleColumnParameterBinding implements ParameterBinding {
//----------------------------------------------------------------------------------------------------------------------
// ParameterBinding Implementation
//----------------------------------------------------------------------------------------------------------------------


        @Override
        public String getColumnName() {
            return columnName;
        }

        @Override
        public String getFacetName() {
            return facet.getName();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object toColumnValue(Object facetValue) {
            return SingleColumnBinding.this.toColumnValue((F)facetValue);
        }
    }
}
