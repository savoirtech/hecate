package com.savoirtech.hecate.cql3.handler;

import com.savoirtech.hecate.cql3.persistence.DeleteContext;

public abstract class AbstractColumnHandler implements ColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean isCascading() {
        return false;
    }

    @Override
    public void getDeletionIdentifiers(Object cassandraValue, DeleteContext context) {
        // Do nothing!
    }
}
