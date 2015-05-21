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

import com.datastax.driver.core.*;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;


public abstract class PojoStatement<P> implements Supplier<PreparedStatement> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final PojoMapping<P> pojoMapping;
    private final Supplier<PreparedStatement> statementSupplier = Suppliers.memoize(this);
    private final Logger logger = LoggerFactory.getLogger(getClass());

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoStatement(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        this.persistenceContext = persistenceContext;
        this.pojoMapping = pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract RegularStatement createStatement();

//----------------------------------------------------------------------------------------------------------------------
// Supplier Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PreparedStatement get() {
        RegularStatement statement = createStatement();
        getLogger().info("{}: {}", getPojoMapping().getPojoClass().getSimpleName(), statement.getQueryString());
        return persistenceContext.prepare(statement);
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    protected Logger getLogger() {
        return logger;
    }

    protected PersistenceContext getPersistenceContext() {
        return persistenceContext;
    }

    protected PojoMapping<P> getPojoMapping() {
        return pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected ResultSet executeStatement(List<Object> parameters, List<Consumer<Statement>> modifiers) {
        getLogger().debug("CQL: {} with parameters {}", statementSupplier.get().getQueryString(), parameters);
        BoundStatement boundStatement = statementSupplier.get().bind(parameters.toArray(new Object[parameters.size()]));
        return persistenceContext.executeStatement(boundStatement, modifiers);
    }
}
