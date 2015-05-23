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
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactory;
import com.savoirtech.hecate.pojo.mapping.PojoMappingFactory;
import com.savoirtech.hecate.pojo.mapping.PojoMappingVerifier;
import com.savoirtech.hecate.pojo.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.def.DefaultPersistenceContext;

public class DefaultPojoDaoFactory implements PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoMappingFactory pojoMappingFactory;
    private final PersistenceContext persistenceContext;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactory(Session session) {
        this(session, null);
    }

    public DefaultPojoDaoFactory(Session session, PojoMappingVerifier verifier) {
        this.persistenceContext = new DefaultPersistenceContext(session);
        this.pojoMappingFactory = new DefaultPojoMappingFactory(verifier);
    }

    public DefaultPojoDaoFactory(PojoMappingFactory pojoMappingFactory, PersistenceContext persistenceContext) {
        this.pojoMappingFactory = pojoMappingFactory;
        this.persistenceContext = persistenceContext;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <I, P> PojoDao<I, P> createPojoDao(Class<P> pojoClass) {
        return new DefaultPojoDao<>(pojoMappingFactory.createPojoMapping(pojoClass), persistenceContext);
    }

    @Override
    public <I, P> PojoDao<I, P> createPojoDao(Class<P> pojoClass, String tableName) {
        return new DefaultPojoDao<>(pojoMappingFactory.createPojoMapping(pojoClass, tableName), persistenceContext);
    }
}
