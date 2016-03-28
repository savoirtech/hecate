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

package com.savoirtech.hecate.pojo.dao.def;

import java.util.concurrent.Executors;

import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.query.def.DefaultPojoQueryContextFactory;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;
import com.savoirtech.hecate.pojo.statement.def.DefaultPojoStatementFactory;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Test;

@Cassandra
public class DefaultPojoDaoFactoryBuilderTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testSimpleBuild() {
        useFactory(builder());
    }

    private void useFactory(DefaultPojoDaoFactoryBuilder factoryBuilder) {
        createTables(SimplePojo.class);
        PojoDao<SimplePojo> dao = factoryBuilder.build().createPojoDao(SimplePojo.class);
        dao.save(new SimplePojo());
    }

    private DefaultPojoDaoFactoryBuilder builder() {
        return new DefaultPojoDaoFactoryBuilder(getSession());
    }

    @Test
    public void testWitHCustomFacetProvider() {
        useFactory(builder().withFacetProvider(new FieldFacetProvider()));
    }

    @Test
    public void testWithCostomeContextFactory() {
        PojoStatementFactory statementFactory = new DefaultPojoStatementFactory(getSession());
        useFactory(builder().withContextFactory(new DefaultPojoQueryContextFactory(getSession(), statementFactory)));
    }

    @Test
    public void testWithCustomExecutor() {
        useFactory(builder().withExecutor(Executors.newSingleThreadExecutor()));
    }

    @Test
    public void testWithMaximumCacheSize() {
        useFactory(builder().withMaximumCacheSize(2000));
    }

    @Test
    public void testWithCustomStatementFactory() {
        useFactory(builder().withStatementFactory(new DefaultPojoStatementFactory(getSession())));
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class SimplePojo extends UuidEntity {
    }
}