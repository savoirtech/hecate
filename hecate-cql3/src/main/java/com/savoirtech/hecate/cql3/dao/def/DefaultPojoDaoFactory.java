package com.savoirtech.hecate.cql3.dao.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;
import com.savoirtech.hecate.cql3.mapping.FieldMappingProvider;
import com.savoirtech.hecate.cql3.mapping.array.ArrayFieldMappingProvider;
import com.savoirtech.hecate.cql3.mapping.list.ListFieldMappingProvider;
import com.savoirtech.hecate.cql3.mapping.map.MapFieldMappingProvider;
import com.savoirtech.hecate.cql3.mapping.scalar.ScalarMappingProvider;
import com.savoirtech.hecate.cql3.mapping.set.SetFieldMappingProvider;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import com.savoirtech.hecate.cql3.naming.NamingConvention;
import com.savoirtech.hecate.cql3.naming.def.DefaultNamingConvention;
import com.savoirtech.hecate.cql3.schema.CreateVerifier;
import com.savoirtech.hecate.cql3.schema.SchemaVerifier;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;
import com.savoirtech.hecate.cql3.type.def.DefaultColumnTypeRegistry;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class DefaultPojoDaoFactory implements PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final List<FieldMappingProvider> fieldMappingProviders;
    private NamingConvention namingConvention = new DefaultNamingConvention();
    private SchemaVerifier verifier = new CreateVerifier();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactory(Session session) {
        this(session, defaultFieldMappingProviders());
    }

    private static FieldMappingProvider[] defaultFieldMappingProviders() {
        final ColumnTypeRegistry registry = new DefaultColumnTypeRegistry();
        return new FieldMappingProvider[]{
                new ScalarMappingProvider(registry),
                new ArrayFieldMappingProvider(registry),
                new ListFieldMappingProvider(registry),
                new SetFieldMappingProvider(registry),
                new MapFieldMappingProvider(registry)
        };
    }

    public DefaultPojoDaoFactory(Session session, FieldMappingProvider... fieldMappingProviders) {
        this.session = session;
        this.fieldMappingProviders = Arrays.asList(fieldMappingProviders);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType) {
        return createPojoDao(pojoType, namingConvention.tableName(pojoType));
    }

    @Override
    public <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType, String tableName) {
        final PojoDescriptor<P> descriptor = getPojoDescriptor(pojoType);
        verifier.verifySchema(session, tableName, descriptor);
        return new DefaultPojoDao<>(session, tableName, descriptor);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private <P> PojoDescriptor<P> getPojoDescriptor(Class<P> pojoType) {
        PojoDescriptor<P> descriptor = new PojoDescriptor<>(pojoType);
        for (Field field : ReflectionUtils.getFields(pojoType)) {
            for (FieldMappingProvider provider : fieldMappingProviders) {
                if (provider.supports(field)) {
                    descriptor.addColumn(namingConvention.columnName(field), namingConvention.isIdentifier(field), provider.createFieldMapping(field, this));
                }
            }
        }
        return descriptor;
    }
}
