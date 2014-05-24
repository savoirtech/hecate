package com.savoirtech.hecate.cql3.meta;

import com.savoirtech.hecate.cql3.mapping.FieldMapping;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class PojoDescriptor<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<P> pojoType;
    private final List<ColumnDescriptor> columns = new LinkedList<>();
    private ColumnDescriptor identifierColumn;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoDescriptor(Class<P> pojoType) {
        this.pojoType = pojoType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public ColumnDescriptor getIdentifierColumn() {
        return identifierColumn;
    }

    public Class<P> getPojoType() {
        return pojoType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void addColumn(String columnName, boolean identifier, FieldMapping mapping) {
        final ColumnDescriptor descriptor = new ColumnDescriptor(columnName, identifier, mapping);
        if (identifier) {
            identifierColumn = descriptor;
            columns.add(0, descriptor);
        }
        else {
            columns.add(descriptor);
        }
    }

    public List<ColumnDescriptor> getColumns() {
        return Collections.unmodifiableList(columns);
    }
}
