package com.savoirtech.hecate.cql3.persistence.def;

import com.datastax.driver.core.Session;
import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMappingFactory;
import com.savoirtech.hecate.cql3.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.cql3.persistence.Persister;
import com.savoirtech.hecate.cql3.persistence.PersisterFactory;
import com.savoirtech.hecate.cql3.schema.CreateVerifier;
import com.savoirtech.hecate.cql3.schema.SchemaVerifier;

import java.util.Map;

public class DefaultPersisterFactory implements PersisterFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final PojoMappingFactory pojoMappingFactory = new DefaultPojoMappingFactory();
    private SchemaVerifier schemaVerifier = new CreateVerifier();

    private final Map<String, Persister> persisters;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static final String key(Class<?> pojoType, String tableName) {
        return pojoType.getCanonicalName() + "@" + tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPersisterFactory(Session session) {
        this.session = session;
        this.persisters = new MapMaker().makeMap();
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
            schemaVerifier.verifySchema(session, pojoMapping);
            persister = new DefaultPersister(session, pojoMapping);
            persisters.put(key, persister);
            if (tableName == null) {
                persisters.put(key(pojoType, pojoMapping.getTableName()), persister);
            }
        }
        return persister;
    }
}
