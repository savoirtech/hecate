package com.savoirtech.hecate.cql3.mapping;

import java.lang.reflect.Field;

public interface FieldMappingFactory {
    FieldMapping createMapping(Field field);
}
