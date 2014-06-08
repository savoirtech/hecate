package com.savoirtech.hecate.cql3.meta;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.mapping.ValueMapping;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class PojoDescriptor<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<P> pojoType;
    private final List<ValueMapping> valueMappings = new LinkedList<>();
    private ValueMapping identifierMapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoDescriptor(Class<P> pojoType) {
        this.pojoType = pojoType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public ValueMapping getIdentifierMapping() {
        return identifierMapping;
    }

    public Class<P> getPojoType() {
        return pojoType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void addMapping(ValueMapping valueMapping) {
        if (valueMapping.isIdentifier()) {
            this.identifierMapping = valueMapping;
            valueMappings.add(0, valueMapping);
        } else {
            valueMappings.add(valueMapping);
        }
    }

    public List<ValueMapping> getValueMappings() {
        return Collections.unmodifiableList(valueMappings);
    }

    public P newInstance() {
        return ReflectionUtils.instantiate(pojoType);
    }
}
