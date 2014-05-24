package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.meta.ColumnDescriptor;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoFindByKey<P, K> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoFindByKey.class);

    private final PreparedStatement preparedStatement;
    private final PojoDescriptor<P> pojoDescriptor;
    private final Session session;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoFindByKey(Session session, String table, PojoDescriptor<P> pojoDescriptor) {
        this.session = session;
        this.preparedStatement = session.prepare(createSelect(table, pojoDescriptor));
        this.pojoDescriptor = pojoDescriptor;
    }

    private static <P> Select.Where createSelect(String table, PojoDescriptor<P> pojoDescriptor) {
        final Select.Selection select = QueryBuilder.select();
        for (ColumnDescriptor columnDescriptor : pojoDescriptor.getColumns()) {
            select.column(columnDescriptor.getColumnName());
        }
        final Select.Where where = select.from(table).where(QueryBuilder.eq(pojoDescriptor.getIdentifierColumn().getColumnName(), QueryBuilder.bindMarker()));
        LOGGER.info("Find statement for entity type {} in table {}: {}", pojoDescriptor.getPojoType().getSimpleName(), table, where);
        return where;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public P find(K key) {
        final BoundStatement boundStatement = preparedStatement.bind();
        // TODO: use the mapping to convert the key value!
        boundStatement.bind(key);
        final ResultSet resultSet = session.execute(boundStatement);
        final Row row = resultSet.one();
        P pojo = pojoDescriptor.newInstance();
        int columnIndex = 0;
        for (ColumnDescriptor descriptor : pojoDescriptor.getColumns()) {
            descriptor.getMapping().extractFrom(pojo, row, columnIndex);
            columnIndex++;
        }
        return pojo;
    }
}
