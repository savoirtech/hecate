package com.savoirtech.hecate.cql3.mapping.def;

import com.savoirtech.hecate.cql3.mapping.FieldMapping;
import com.savoirtech.hecate.cql3.mapping.FieldMappingFactory;
import com.savoirtech.hecate.cql3.mapping.FieldMappingProvider;
import com.savoirtech.hecate.cql3.mapping.array.ArrayFieldMappingProvider;
import com.savoirtech.hecate.cql3.mapping.list.ListFieldMappingProvider;
import com.savoirtech.hecate.cql3.mapping.map.MapFieldMappingProvider;
import com.savoirtech.hecate.cql3.mapping.scalar.ScalarMappingProvider;
import com.savoirtech.hecate.cql3.mapping.set.SetFieldMappingProvider;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;
import com.savoirtech.hecate.cql3.type.def.DefaultColumnTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class DefaultFieldMappingFactory implements FieldMappingFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFieldMappingFactory.class);

    private ColumnTypeRegistry columnTypeRegistry = new DefaultColumnTypeRegistry();
    private final List<FieldMappingProvider> fieldMappingProviders;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultFieldMappingFactory() {
        this.fieldMappingProviders = Arrays.asList(defaultFieldMappingProviders(columnTypeRegistry));
    }

    private static FieldMappingProvider[] defaultFieldMappingProviders(ColumnTypeRegistry registry) {
        return new FieldMappingProvider[]{
                new ScalarMappingProvider(registry),
                new ArrayFieldMappingProvider(registry),
                new ListFieldMappingProvider(registry),
                new SetFieldMappingProvider(registry),
                new MapFieldMappingProvider(registry)
        };
    }

    public DefaultFieldMappingFactory(FieldMappingProvider... fieldMappingProviders) {
        this.fieldMappingProviders = Arrays.asList(fieldMappingProviders);
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMappingFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public FieldMapping createMapping(Field field) {
        for (FieldMappingProvider provider : fieldMappingProviders) {
            if (provider.supports(field)) {
                return provider.createFieldMapping(field);
            }
        }
        LOGGER.warn("Unable to find mapping for field {}!", field);
        return null;
    }
}
