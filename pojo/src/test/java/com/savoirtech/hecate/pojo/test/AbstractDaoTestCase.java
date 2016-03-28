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

package com.savoirtech.hecate.pojo.test;

import java.util.Arrays;

import com.datastax.driver.core.schemabuilder.Create;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoBindingFactory;
import com.savoirtech.hecate.pojo.binding.def.DefaultPojoBindingFactory;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactory;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.pojo.dao.listener.VerifySchemaListener;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.naming.def.DefaultNamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.query.def.DefaultPojoQueryContextFactory;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;
import com.savoirtech.hecate.pojo.statement.def.DefaultPojoStatementFactory;
import com.savoirtech.hecate.test.CassandraTestCase;

public abstract class AbstractDaoTestCase extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final FacetProvider FACET_PROVIDER = new FieldFacetProvider();


    private final NamingStrategy namingStrategy = new DefaultNamingStrategy();
    private final FacetProvider facetProvider = new FieldFacetProvider();
    private final ConverterRegistry converterRegistry = new DefaultConverterRegistry();
    private final PojoBindingFactory bindingFactory = new DefaultPojoBindingFactory(facetProvider, converterRegistry, namingStrategy);
    private final Supplier<PojoStatementFactory> statementFactory = Suppliers.memoize(() -> new DefaultPojoStatementFactory(getSession()));
    private final Supplier<PojoQueryContextFactory> contextFactory = Suppliers.memoize(() -> new DefaultPojoQueryContextFactory(getSession(), statementFactory.get()));
    private final Supplier<PojoDaoFactory> daoFactory = Suppliers.memoize(() -> new DefaultPojoDaoFactory(getSession(), bindingFactory, statementFactory.get(), contextFactory.get(), namingStrategy).addListener(new VerifySchemaListener(getSession())));

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    protected PojoBindingFactory getBindingFactory() {
        return bindingFactory;
    }

    public ConverterRegistry getConverterRegistry() {
        return converterRegistry;
    }

    public FacetProvider getFacetProvider() {
        return facetProvider;
    }

    protected NamingStrategy getNamingStrategy() {
        return namingStrategy;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void assertHecateException(String message, Runnable runnable) {
        try {
            runnable.run();
            fail("Should have thrown HecateException!");
        }
        catch(HecateException e) {
            assertEquals(message, e.getMessage());
        }
    }

    protected <P> PojoDao<P> createPojoDao(Class<P> pojoType) {
        createTables(pojoType);
        return daoFactory.get().createPojoDao(pojoType);
    }

    protected void createTable(Class<?> pojoType, String tableName) {
        Create create = bindingFactory.createPojoBinding(pojoType).createTable(tableName);
        logger.debug("Creating \"{}\" table for class \"{}\":\n\t{}\n", tableName, pojoType.getSimpleName(), create);
        getSession().execute(create);
    }

    protected void createTables(Class<?>... pojoTypes) {
        Arrays.stream(pojoTypes).forEach(pojoType -> createTable(pojoType, namingStrategy.getTableName(pojoType)));
    }

    protected PojoQueryContextFactory getContextFactory() {
        return contextFactory.get();
    }

    protected Facet getFacet(Class<?> pojoType, String name) {
        return FACET_PROVIDER.getFacetsAsMap(pojoType).get(name);
    }

    protected <P> PojoBinding<P> getPojoBinding(Class<P> pojoType) {
        return bindingFactory.createPojoBinding(pojoType);
    }
}
