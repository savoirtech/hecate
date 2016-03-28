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

package com.savoirtech.hecate.pojo.query.finder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.query.AbstractPojoQuery;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;

public class FindByKeyQuery<P> extends AbstractPojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FindByKeyQuery(Session session, PojoBinding<P> binding, PojoQueryContextFactory contextFactory, PreparedStatement statement, Executor executor) {
        super(session, binding, contextFactory, statement, executor);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object[] convertParameters(Object[] params) {
        List<Object> converted = getBinding().getKeyBinding().getKeyParameters(Arrays.asList(params));
        return converted.toArray(new Object[converted.size()]);
    }
}
