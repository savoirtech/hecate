package com.savoirtech.hecate.cql3.exception;

public class ValueConverterNotFoundException extends HecateException {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ValueConverterNotFoundException(Class<?> targetType) {
        super(String.format("Unable to find ValueConverter for values of type %s.", targetType.getName()));
    }
}
