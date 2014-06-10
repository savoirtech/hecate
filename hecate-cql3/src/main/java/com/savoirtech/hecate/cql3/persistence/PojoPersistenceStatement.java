package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.util.CassandraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class PojoPersistenceStatement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoPersistenceStatement.class);

    private final Session session;
    private final PreparedStatement preparedStatement;
    private final PojoMapping pojoMapping;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    protected static Select.Selection pojoSelect(PojoMapping pojoMapping) {
        final Select.Selection select = QueryBuilder.select();
        for (FacetMapping mapping : pojoMapping.getFacetMappings()) {
            select.column(mapping.getFacetMetadata().getColumnName());
        }
        return select;
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    protected PojoPersistenceStatement(Session session, RegularStatement statement, PojoMapping pojoMapping) {
        this.session = session;
        this.preparedStatement = session.prepare(statement);
        this.pojoMapping = pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public PojoMapping getPojoMapping() {
        return pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

//    protected List<FacetMapping> allColumns() {
//        return pojoMapping.getFacetMappings();
//    }

//    protected Object cassandraValue(P pojo, FacetMapping mapping) {
//        return mapping.getConverter().toCassandraValue(mapping.getFacet().get(pojo), null);
//    }
//
//    protected List<Object> cassandraValues(P pojo, List<FacetMapping> mappings) {
//        final List<Object> values = new ArrayList<>(mappings.size());
//        for (FacetMapping mapping : mappings) {
//            values.add(mapping.getConverter().toCassandraValue(mapping.getFacet().get(pojo), null));
//        }
//        return values;
//    }

    protected ResultSet executeWithArgs(Object... parameters) {
        return executeWithList(Arrays.asList(parameters));
    }

    protected ResultSet executeWithList(List<Object> parameters) {
        LOGGER.debug("CQL: {} with parameters {}...", preparedStatement.getQueryString(), parameters);
        return session.execute(preparedStatement.bind(parameters.toArray(new Object[parameters.size()])));
    }

    //    protected FacetMapping identifierMapping() {
//        return pojoMapping.getIdentifierMapping();
//    }
//
//    protected List<P> list(ResultSet resultSet) {
//        List<Row> rows = resultSet.all();
//        final List<P> pojos = new ArrayList<>(rows.size());
//        for (Row row : rows) {
//            pojos.add(mapPojoFromRow(row));
//        }
//        return pojos;
//    }
//
    @SuppressWarnings("unchecked")
    protected Object mapPojoFromRow(Object pojo, Row row, QueryContext context) {
        int columnIndex = 0;
        for (FacetMapping mapping : pojoMapping.getFacetMappings()) {
            Object columnValue = CassandraUtils.getValue(row, columnIndex, mapping.getColumnHandler().getColumnType());
            mapping.getFacetMetadata().getFacet().set(pojo, mapping.getColumnHandler().getFacetValue(columnValue, context));
            columnIndex++;
        }
        return pojo;
    }

    protected Object one(Object pojo, ResultSet resultSet, QueryContext context) {
        Row row = resultSet.one();
        return row == null ? null : mapPojoFromRow(pojo, row, context);
    }
}
