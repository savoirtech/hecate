package com.savoirtech.hecate.cql3.handler.scalar;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.AbstractSetHandler;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

public class ScalarSetHandler extends AbstractSetHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverter elementConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ScalarSetHandler(ValueConverter elementConverter) {
        super(elementConverter.getDataType());
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
        return null;
    }
}
