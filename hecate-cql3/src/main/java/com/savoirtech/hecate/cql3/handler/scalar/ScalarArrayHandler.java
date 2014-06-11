package com.savoirtech.hecate.cql3.handler.scalar;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.AbstractArrayHandler;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

public class ScalarArrayHandler extends AbstractArrayHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverter elementConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ScalarArrayHandler(Class<?> elementType, ValueConverter elementConverter) {
        super(elementType, elementConverter.getDataType());
        this.elementConverter = elementConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object toCassandraElement(Object facetElement, SaveContext context) {
        return elementConverter.toCassandraValue(facetElement);
    }

    @Override
    protected Object toFacetElement(Object cassandraElement, QueryContext context) {
        return elementConverter.fromCassandraValue(cassandraElement);
    }

    @Override
    public Object getWhereClauseValue(Object parameterValue) {
        return elementConverter.toCassandraValue(parameterValue);
    }
}
