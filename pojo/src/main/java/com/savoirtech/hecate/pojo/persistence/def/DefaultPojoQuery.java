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

package com.savoirtech.hecate.pojo.persistence.def;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.pojo.mapping.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.row.HydratorRowMapper;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DefaultPojoQuery<P> extends PojoStatement<P> implements PojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Select.Where selectWhere;
    private final List<FacetMapping> parameterMappings;
    private final List<InjectedParameter> injectedParameters;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQuery(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping, Select.Where selectWhere, List<FacetMapping> parameterMappings, List<InjectedParameter> injectedParameters) {
        super(persistenceContext, pojoMapping);
        this.selectWhere = selectWhere;
        this.parameterMappings = parameterMappings;
        this.injectedParameters = injectedParameters;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoQuery Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public final MappedQueryResult<P> execute(Object... parameters) {
        if(parameters.length != parameterMappings.size()) {
            throw new HecateException("Expected %d parameters, but only received %d.", parameterMappings.size(), parameters.length);
        }

        List<Object> cassandraValues = new ArrayList<>(parameterMappings.size() + injectedParameters.size());
        int index = 0;
        for (FacetMapping mapping : parameterMappings) {
            cassandraValues.add(mapping.getColumnType().toCassandraValue(parameters[index]));
            index++;
        }
        cassandraValues = injected(cassandraValues);
        return new MappedQueryResult<>(executeStatement(cassandraValues, Collections.emptyList()),new HydratorRowMapper<>(getPojoMapping(),getPersistenceContext().createHydrator()));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected RegularStatement createStatement() {
        return selectWhere;
    }

    private List<Object> injected(List<Object> parameters) {
        if (injectedParameters.isEmpty()) {
            return parameters;
        }
        final List<Object> injected = new ArrayList<>(parameters.size() + injectedParameters.size());
        injected.addAll(parameters);
        for (InjectedParameter parameter : injectedParameters) {
            parameter.injectInto(injected);
        }
        return injected;
    }
}
