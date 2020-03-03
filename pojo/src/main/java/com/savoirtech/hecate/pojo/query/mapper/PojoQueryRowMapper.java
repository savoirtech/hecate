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

package com.savoirtech.hecate.pojo.query.mapper;

import com.datastax.oss.driver.api.core.cql.Row;
import com.savoirtech.hecate.core.mapping.RowMapper;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

public class PojoQueryRowMapper<P> implements RowMapper<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoBinding<P> binding;
    private final PojoQueryContext context;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoQueryRowMapper(PojoBinding<P> binding, PojoQueryContext context) {
        this.binding = binding;
        this.context = context;
    }

//----------------------------------------------------------------------------------------------------------------------
// RowMapper Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public P map(Row row) {
        P pojo = binding.createPojo();
        binding.injectValues(pojo, row, context);
        return pojo;
    }

    @Override
    public void mappingComplete() {
        context.awaitCompletion();
    }
}
