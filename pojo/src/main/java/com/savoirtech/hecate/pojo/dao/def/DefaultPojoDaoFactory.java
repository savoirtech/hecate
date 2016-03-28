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

package com.savoirtech.hecate.pojo.dao.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoBindingFactory;
import com.savoirtech.hecate.pojo.binding.def.DefaultPojoBindingFactory;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactory;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.naming.def.DefaultNamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.query.def.DefaultPojoQueryContextFactory;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;
import com.savoirtech.hecate.pojo.statement.def.DefaultPojoStatementFactory;

public class DefaultPojoDaoFactory implements PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;

    private final PojoBindingFactory bindingFactory;
    private final PojoStatementFactory statementFactory;
    private final PojoQueryContextFactory contextFactory;
    private final NamingStrategy namingStrategy;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactory(Session session) {
        this.session = session;
        this.statementFactory = new DefaultPojoStatementFactory(session);
        this.namingStrategy = new DefaultNamingStrategy();
        this.bindingFactory = new DefaultPojoBindingFactory(new FieldFacetProvider(), new DefaultConverterRegistry(), namingStrategy);
        this.contextFactory = new DefaultPojoQueryContextFactory(session, statementFactory);
    }

    public DefaultPojoDaoFactory(Session session, PojoBindingFactory bindingFactory, PojoStatementFactory statementFactory, PojoQueryContextFactory contextFactory, NamingStrategy namingStrategy) {
        this.session = session;
        this.bindingFactory = bindingFactory;
        this.statementFactory = statementFactory;
        this.contextFactory = contextFactory;
        this.namingStrategy = namingStrategy;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <P> PojoDao<P> createPojoDao(Class<P> pojoClass) {
        return createPojoDao(pojoClass, namingStrategy.getTableName(pojoClass));
    }

    @Override
    public <P> PojoDao<P> createPojoDao(Class<P> pojoClass, String tableName) {
        PojoBinding<P> pojoBinding = bindingFactory.createPojoBinding(pojoClass);
        pojoBinding.verifySchema(session, tableName);
        return new DefaultPojoDao<>(session, pojoBinding, tableName, statementFactory, contextFactory);
    }
}
