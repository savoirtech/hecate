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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.savoirtech.hecate.test.CassandraSingleton;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.PojoBindingFactory;
import com.savoirtech.hecate.pojo.binding.def.DefaultPojoBindingFactory;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactory;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactoryBuilder;
import com.savoirtech.hecate.pojo.dao.listener.CreateSchemaListener;
import com.savoirtech.hecate.pojo.dao.listener.VerifySchemaListener;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.naming.def.DefaultNamingStrategy;

public abstract class AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected static final Executor EXECUTOR = Executors.newSingleThreadExecutor();

    private final NamingStrategy namingStrategy = new DefaultNamingStrategy();
    private final FacetProvider facetProvider = new FieldFacetProvider();
    private final ConverterRegistry converterRegistry = new DefaultConverterRegistry();
    private final PojoBindingFactory bindingFactory = new DefaultPojoBindingFactory(facetProvider, converterRegistry, namingStrategy);

    private final Supplier<PojoDaoFactory> daoFactory = Suppliers.memoize(() ->
            new DefaultPojoDaoFactoryBuilder(CassandraSingleton.getSession())
                    .withBindingFactory(bindingFactory)
                    .withNamingStrategy(namingStrategy)
                    .withConverterRegistry(converterRegistry)
                    .withThreadPoolSize(1)
                    .withListener(new CreateSchemaListener(CassandraSingleton.getSession()))
                    .withListener(new VerifySchemaListener(CassandraSingleton.getSession()))
                    .build());

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
        } catch (HecateException e) {
            assertEquals(message, e.getMessage());
        }
    }

    protected <P> PojoDao<P> createPojoDao(Class<P> pojoType) {
        return daoFactory.get().createPojoDao(pojoType);
    }

    protected <P> PojoDao<P> createPojoDao(Class<P> pojoType, String tableName) {
        return daoFactory.get().createPojoDao(pojoType, tableName);
    }

    protected Facet getFacet(Class<?> pojoType, String name) {
        return facetProvider.getFacetsAsMap(pojoType).get(name);
    }
}
