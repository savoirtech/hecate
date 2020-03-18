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

package com.savoirtech.hecate.pojo.query.def;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.query.AbstractPojoQuery;
import com.savoirtech.hecate.pojo.query.ParameterConverter;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;

public class DefaultPojoQuery<P> extends AbstractPojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final List<ParameterConverter> parameterConverters;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQuery(CqlSession session, PojoBinding<P> binding, PojoQueryContextFactory contextFactory, Select select, List<ParameterConverter> parameterConverters, Executor executor) {
        super(session, binding, contextFactory, select, executor);
        this.parameterConverters = parameterConverters;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object[] convertParameters(Object[] params) {
        Iterator<Object> parameterIterator = Iterators.forArray(params);
        List<Object> converted = parameterConverters.stream().map(parameterBinding -> parameterBinding.convertParameter(parameterIterator)).collect(Collectors.toList());
        return converted.toArray(new Object[converted.size()]);
    }
}
