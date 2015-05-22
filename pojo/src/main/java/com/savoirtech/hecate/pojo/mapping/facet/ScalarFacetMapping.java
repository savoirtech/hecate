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

package com.savoirtech.hecate.pojo.mapping.facet;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.mapping.column.ColumnType;

import java.util.function.Function;

public class ScalarFacetMapping extends AbstractFacetMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Converter elementConverter;
    private final DataType dataType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public ScalarFacetMapping(Facet facet, ColumnType<?,?> columnType, Converter elementConverter) {
        super(facet, (ColumnType<Object,Object>)columnType);
        this.elementConverter = elementConverter;
        this.dataType = columnType.getDataType(elementConverter.getDataType());
    }

//----------------------------------------------------------------------------------------------------------------------
// FacetMapping Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void accept(FacetMappingVisitor visitor) {
        visitor.visitScalar(this);
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public boolean isReference() {
        return false;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Function<Object, Object> elementColumnValue() {
        return elementConverter::toColumnValue;
    }

    public void setFacetValue(Object pojo, Object columnValue) {
        if(columnValue == null) {
            getFacet().setValue(pojo, null);
        }
        else {
            getFacet().setValue(pojo, getColumnType().getFacetValue(columnValue, elementConverter::toFacetValue, getFacet().getType().getElementType().getRawType()));
        }
    }
}
