/*
 * Copyright (c) 2014. Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.dao.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;
import com.savoirtech.hecate.cql3.persistence.PersisterFactory;
import com.savoirtech.hecate.cql3.persistence.def.DefaultPersisterFactory;

public class DefaultPojoDaoFactory implements PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersisterFactory persisterFactory;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactory(Session session) {
        this.persisterFactory = new DefaultPersisterFactory(session);
    }

    public DefaultPojoDaoFactory(PersisterFactory persisterFactory) {
        this.persisterFactory = persisterFactory;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType) {
        return createPojoDao(pojoType, null);
    }

    @Override
    public <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType, String tableName) {
        return new DefaultPojoDao<>(persisterFactory, pojoType, tableName);
    }
}
