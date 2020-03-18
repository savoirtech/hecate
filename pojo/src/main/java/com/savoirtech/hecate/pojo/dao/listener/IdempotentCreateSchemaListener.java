/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.dao.listener;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.Optional;
import java.util.UUID;

import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryEvent;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryListener;
import com.savoirtech.hecate.pojo.exception.SchemaVerificationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdempotentCreateSchemaListener implements PojoDaoFactoryListener {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final String DEFAULT_TABLE_NAME = "hecate_schema_idempotency";
    public static final String KEY = "key";
    public static final String CLAIM_ID_COL = "claim_id";
    private static final Logger LOGGER = LoggerFactory.getLogger(IdempotentCreateSchemaListener.class);
    private final CqlSession session;
    private final PreparedStatement insertStatement;
    private final PreparedStatement selectStatement;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static SimpleStatement createIdempotencyTable() {
        return createIdempotencyTable(DEFAULT_TABLE_NAME);
    }

    public static SimpleStatement createIdempotencyTable(String tableName) {
        return createTable(tableName)
                .withPartitionKey(KEY, DataTypes.TEXT)
                .withColumn(CLAIM_ID_COL, DataTypes.TEXT)
                .build();
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public IdempotentCreateSchemaListener(CqlSession session) {
        this(session, DEFAULT_TABLE_NAME);
    }

    public IdempotentCreateSchemaListener(CqlSession session, String tableName) {
        this.session = session;
        verifyIdempotencyTable(session, tableName);

        insertStatement = session.prepare(
                insertInto(tableName)
                        .value(KEY, bindMarker())
                        .value(CLAIM_ID_COL, bindMarker())
                        .ifNotExists()
                        .build());

        selectStatement = session.prepare(
                selectFrom(tableName)
                        .column(CLAIM_ID_COL)
                        .whereColumn(KEY)
                        .isEqualTo(bindMarker())
                        .build());
    }

    private static void verifyIdempotencyTable(CqlSession session, String tableName) {
        Optional<KeyspaceMetadata> keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get());
        Optional<TableMetadata> table = keyspace.get().getTable(tableName);
        if (!table.isPresent()) {
            final SimpleStatement create = createIdempotencyTable(tableName);
            LOGGER.error("Schema idempotency table \"{}\" does not exist, please create it using the following CQL:\n\t{}", tableName, create);
            throw new SchemaVerificationException("Schema idempotency table \"%s\" does not exist, see logs for instructions.", tableName);
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactoryListener Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <P> void pojoDaoCreated(PojoDaoFactoryEvent<P> event) {
        String tableName = event.getTableName();
        Schema schema = new Schema();
        event.getPojoBinding().describe(schema.createTable(tableName), schema);

        schema.items().forEach(schemaItem -> {
            final String key = schemaItem.getKey();
            final String claimId = UUID.randomUUID().toString();
            session.execute(insertStatement.bind(key, claimId));
            Row row = session.execute(selectStatement.bind(key)).one();
            if (StringUtils.equals(claimId, row.getString(0))) {
                final SimpleStatement statement = schemaItem.createStatement();
                LOGGER.info("Creating table(s) to support \"{}\":\n\t{}\n", event.getPojoBinding().getPojoType().getSimpleName(), statement);
                session.execute(statement);
            }
        });
    }
}
