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

package com.savoirtech.hecate.pojo.mapping.element;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.Hydrator;

public class ConverterElementHandler implements ElementHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Converter converter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ConverterElementHandler(Converter converter) {
        this.converter = converter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ElementHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return converter.getDataType();
    }

    @Override
    public Object getInsertValue(Object facetValue, Dehydrator dehydrator) {
        return converter.toCassandraValue(facetValue);
    }

    @Override
    public Object getParameterValue(Object facetValue) {
        return converter.toCassandraValue(facetValue);
    }

    @Override
    public boolean isCascadable() {
        return false;
    }

    @Override
    public void resolveElements(Iterable<Object> cassandraValue, Hydrator hydrator,ElementInjector injector) {
        injector.injectElement(converter::fromCassandraValue);
    }
}
