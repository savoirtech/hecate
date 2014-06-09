package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import com.savoirtech.hecate.cql3.util.CassandraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PojoPersistenceStatement<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoPersistenceStatement.class);

    private final Session session;
    private final PreparedStatement preparedStatement;
    private final PojoDescriptor<P> pojoDescriptor;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    protected static <P> Select.Selection pojoSelect(PojoDescriptor<P> pojoDescriptor) {
        final Select.Selection select = QueryBuilder.select();
        for (FacetMapping mapping : pojoDescriptor.getFacetMappings()) {
            select.column(mapping.getColumnName());
        }
        return select;
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    protected PojoPersistenceStatement(Session session, RegularStatement statement, PojoDescriptor<P> pojoDescriptor) {
        this.session = session;
        this.preparedStatement = session.prepare(statement);
        this.pojoDescriptor = pojoDescriptor;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected List<FacetMapping> allColumns() {
        return pojoDescriptor.getFacetMappings();
    }

    protected Object cassandraValue(P pojo, FacetMapping mapping) {
        return mapping.getConverter().toCassandraValue(mapping.getFacet().get(pojo), null);
    }

    protected List<Object> cassandraValues(P pojo, List<FacetMapping> mappings) {
        final List<Object> values = new ArrayList<>(mappings.size());
        for (FacetMapping mapping : mappings) {
            values.add(mapping.getConverter().toCassandraValue(mapping.getFacet().get(pojo), null));
        }
        return values;
    }

    protected ResultSet executeWithArgs(Object... parameters) {
        return executeWithList(Arrays.asList(parameters));
    }

    protected ResultSet executeWithList(List<Object> parameters) {
        LOGGER.debug("CQL: {} with parameters {}...", preparedStatement.getQueryString(), parameters);
        return session.execute(preparedStatement.bind(parameters.toArray(new Object[parameters.size()])));
    }

    protected FacetMapping identifierMapping() {
        return pojoDescriptor.getIdentifierMapping();
    }

    protected List<P> list(ResultSet resultSet) {
        List<Row> rows = resultSet.all();
        final List<P> pojos = new ArrayList<>(rows.size());
        for (Row row : rows) {
            pojos.add(mapPojoFromRow(row));
        }
        return pojos;
    }

    protected P mapPojoFromRow(Row row) {
        P pojo = pojoDescriptor.newInstance();
        int columnIndex = 0;
        for (FacetMapping mapping : pojoDescriptor.getFacetMappings()) {
            Object columnValue = CassandraUtils.getValue(row, columnIndex, mapping.getConverter().getDataType());
            // TODO: Use Hydrator!
            mapping.getFacet().set(pojo, mapping.getConverter().fromCassandraValue(columnValue, null));
            columnIndex++;
        }
        return pojo;
    }

    protected P one(ResultSet resultSet) {
        Row row = resultSet.one();
        return row == null ? null : mapPojoFromRow(row);
    }
}
