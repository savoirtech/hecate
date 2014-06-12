/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.cql3.persistence.def;

import com.datastax.driver.core.Session;
import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMappingFactory;
import com.savoirtech.hecate.cql3.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.cql3.persistence.Persister;
import com.savoirtech.hecate.cql3.persistence.PersisterFactory;

import java.util.Map;

public class DefaultPersisterFactory implements PersisterFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private PojoMappingFactory pojoMappingFactory;


    private final Map<String, Persister> persisters;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static String key(Class<?> pojoType, String tableName) {
        return pojoType.getCanonicalName() + "@" + tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPersisterFactory(Session session) {
        this.session = session;
        this.persisters = new MapMaker().makeMap();
        this.pojoMappingFactory = new DefaultPojoMappingFactory(session);
    }

//----------------------------------------------------------------------------------------------------------------------
// PersisterFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Persister getPersister(Class<?> pojoType, String tableName) {
        final String key = key(pojoType, tableName);
        Persister persister = persisters.get(key);
        if (persister == null) {
            final PojoMapping pojoMapping = pojoMappingFactory.getPojoMapping(pojoType, tableName);
            persister = new DefaultPersister(session, pojoMapping);
            persisters.put(key, persister);
            if (tableName == null) {
                persisters.put(key(pojoType, pojoMapping.getTableName()), persister);
            }
        }
        return persister;
    }
}
