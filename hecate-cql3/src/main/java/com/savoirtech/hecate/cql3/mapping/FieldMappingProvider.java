package com.savoirtech.hecate.cql3.mapping;

import java.lang.reflect.Field;

public interface FieldMappingProvider {

    boolean supports(Field field);

    FieldMapping createFieldMapping(Field field);
}
