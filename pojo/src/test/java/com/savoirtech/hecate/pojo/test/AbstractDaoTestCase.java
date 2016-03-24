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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Before;

public abstract class AbstractDaoTestCase extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private DefaultPojoDaoFactory factory;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void createDaoFactory() {
        Session session = getSession();
        this.factory = new DefaultPojoDaoFactory(session);
    }

    protected <P> PojoDao<P> createPojoDao(Class<P> pojoType) {
        createTable(pojoType);
        return factory.createPojoDao(pojoType);
    }

    protected void createTable(Class<?> pojoType) {
        createTable(pojoType, factory.getNamingStrategy().getTableName(pojoType));
    }

    protected void createTable(Class<?> pojoType, String tableName) {
        Create create = factory.getBindingFactory().createPojoBinding(pojoType).createTable(tableName);
        logger.info("Creating \"{}\" table for class \"{}\":\n\t{}\n", tableName, pojoType.getSimpleName(), create);
        getSession().execute(create);
    }
}
