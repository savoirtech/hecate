package com.savoirtech.hecate.cql3.meta.def;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.mapping.FieldMapping;
import com.savoirtech.hecate.cql3.mapping.FieldMappingFactory;
import com.savoirtech.hecate.cql3.mapping.def.DefaultFieldMappingFactory;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import com.savoirtech.hecate.cql3.meta.PojoDescriptorFactory;
import com.savoirtech.hecate.cql3.naming.NamingConvention;

import java.lang.reflect.Field;

public class DefaultPojoDescriptorFactory implements PojoDescriptorFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private FieldMappingFactory fieldMappingFactory = new DefaultFieldMappingFactory();
    private NamingConvention namingConvention;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDescriptorFactory(NamingConvention namingConvention) {
        this.namingConvention = namingConvention;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDescriptorFactory Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public <P> PojoDescriptor<P> getPojoDescriptor(Class<P> pojoType) {
        PojoDescriptor<P> descriptor = new PojoDescriptor<>(pojoType);
        for (Field field : ReflectionUtils.getFields(pojoType)) {
            final FieldMapping fieldMapping = fieldMappingFactory.createMapping(field);
            if (fieldMapping != null) {
                descriptor.addColumn(namingConvention.columnName(field), namingConvention.isIdentifier(field), fieldMapping);
            }
        }
        return descriptor;
    }
}
