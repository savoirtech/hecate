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
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.test.CassandraTestCase;

public abstract class AbstractDaoTestCase extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final FacetProvider FACET_PROVIDER = new FieldFacetProvider();

    private final Supplier<DefaultPojoDaoFactory> daoFactory = Suppliers.memoize(() -> new DefaultPojoDaoFactory(getSession()));

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected <P> PojoDao<P> createPojoDao(Class<P> pojoType) {
        createTables(pojoType);
        return daoFactory.get().createPojoDao(pojoType);
    }

    protected <P> PojoBinding<P> getPojoBinding(Class<P> pojoType) {
        return daoFactory.get().getBindingFactory().createPojoBinding(pojoType);
    }

    protected PojoQueryContextFactory getContextFactory() {
        return daoFactory.get().getContextFactory();
    }

    protected NamingStrategy getNamingStrategy() {
        return daoFactory.get().getNamingStrategy();
    }

    protected void createTables(Class<?>... pojoTypes) {
        Arrays.stream(pojoTypes).forEach(pojoType -> createTable(pojoType, daoFactory.get().getNamingStrategy().getTableName(pojoType)));
    }

    protected void createTable(Class<?> pojoType, String tableName) {
        Create create = daoFactory.get().getBindingFactory().createPojoBinding(pojoType).createTable(tableName);
        logger.info("Creating \"{}\" table for class \"{}\":\n\t{}\n", tableName, pojoType.getSimpleName(), create);
        getSession().execute(create);
    }

    protected Facet getFacet(Class<?> pojoType, String name) {
        return FACET_PROVIDER.getFacetsAsMap(pojoType).get(name);
    }
}
