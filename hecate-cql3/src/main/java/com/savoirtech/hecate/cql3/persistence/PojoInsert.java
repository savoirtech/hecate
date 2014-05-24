package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.savoirtech.hecate.cql3.meta.ColumnDescriptor;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoInsert<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoInsert.class);
    private final Session session;
    private final PreparedStatement preparedStatement;
    private final PojoDescriptor<P> pojoDescriptor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoInsert(Session session, String table, PojoDescriptor<P> pojoDescriptor) {
        this.session = session;
        this.pojoDescriptor = pojoDescriptor;
        this.preparedStatement = session.prepare(createInsert(table, pojoDescriptor));
    }

    private static <P> Insert createInsert(String table, PojoDescriptor<P> pojoDescriptor) {
        final Insert insert = QueryBuilder.insertInto(table);
        for (ColumnDescriptor columnDescriptor : pojoDescriptor.getColumns()) {
            insert.value(columnDescriptor.getColumnName(), QueryBuilder.bindMarker());
        }
        LOGGER.info("Insert statement for entity type {} in table {}: {}", pojoDescriptor.getPojoType().getSimpleName(), table, insert);
        return insert;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void execute(P pojo) {
        BoundStatement boundStatement = preparedStatement.bind();
        int parameterIndex = 0;
        for (ColumnDescriptor columnDescriptor : pojoDescriptor.getColumns()) {
            columnDescriptor.getMapping().bindTo(pojo, boundStatement, parameterIndex);
            parameterIndex++;
        }
        session.execute(boundStatement);
    }
}
