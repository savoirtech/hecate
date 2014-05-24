package com.savoirtech.hecate.cql3.mapping;

import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;

import java.lang.reflect.Field;

public interface FieldMappingProvider {

    boolean supports(Field field);

    FieldMapping createFieldMapping(Field field, PojoDaoFactory factory);
}
