package com.savoirtech.hecate.cql3.dao.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.annotations.TableName;
import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import com.savoirtech.hecate.cql3.meta.PojoDescriptorFactory;
import com.savoirtech.hecate.cql3.meta.def.DefaultPojoDescriptorFactory;
import com.savoirtech.hecate.cql3.schema.CreateVerifier;
import com.savoirtech.hecate.cql3.schema.SchemaVerifier;

public class DefaultPojoDaoFactory implements PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private SchemaVerifier verifier = new CreateVerifier();
    private PojoDescriptorFactory pojoDescriptorFactory = new DefaultPojoDescriptorFactory();

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static String tableName(Class<?> pojoType) {
        TableName annot = pojoType.getAnnotation(TableName.class);
        return annot == null ? pojoType.getSimpleName() : annot.value();
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactory(Session session) {
        this.session = session;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType) {
        return createPojoDao(pojoType, tableName(pojoType));
    }

    @Override
    public <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType, String tableName) {
        final PojoDescriptor<P> descriptor = pojoDescriptorFactory.getPojoDescriptor(pojoType);
        verifier.verifySchema(session, tableName, descriptor);
        return new DefaultPojoDao<>(session, tableName, descriptor);
    }
}
