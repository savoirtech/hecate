/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.migrator;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.migrator.exception.SchemaMigrationException;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;

public class SchemaMigrationMetadataRepositoryImpl implements SchemaMigrationMetadataRepository {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final String METADATA_TABLE = "hecate_migration_metadata";
    public static final String ID_COL = "id";
    public static final String INDEX_COL = "ndx";
    public static final String TOKEN_COL = "sync_token";
    public static final String STATUS_COL = "status";
    public static final String FINGERPRINT_COL = "fingerprint";

    private final Session session;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static void createMigrationTable(Session session) {
        session.execute(createTable(METADATA_TABLE)
                .addPartitionKey(ID_COL, varchar())
                .addColumn(INDEX_COL, cint())
                .addColumn(TOKEN_COL, varchar())
                .addColumn(STATUS_COL, varchar())
                .addColumn(FINGERPRINT_COL, varchar()));
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SchemaMigrationMetadataRepositoryImpl(Session session) {
        this.session = session;
        KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
        if (keyspace.getTable(METADATA_TABLE) == null) {
            throw new SchemaMigrationException("Schema migration metadata table \"%s\" not found.", METADATA_TABLE);
        }
        session.getCluster().getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }

//----------------------------------------------------------------------------------------------------------------------
// SchemaMigrationMetadataRepository Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void create(SchemaMigrationMetadata metadata) {
        session.execute(
                insertInto(METADATA_TABLE)
                        .ifNotExists()
                        .value(ID_COL, metadata.getId())
                        .value(INDEX_COL, metadata.getIndex())
                        .value(TOKEN_COL, metadata.getToken())
                        .value(STATUS_COL, metadata.getStatus().name())
                        .value(FINGERPRINT_COL, metadata.getFingerprint()));
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public SchemaMigrationMetadata retrieve(String id) {
        ResultSet resultSet = session.execute(select(ID_COL, INDEX_COL, TOKEN_COL, STATUS_COL, FINGERPRINT_COL).from(METADATA_TABLE).where(eq(ID_COL, id)));
        return new MappedQueryResult<>(resultSet, row -> {
            SchemaMigrationMetadata metadata = new SchemaMigrationMetadata();
            metadata.setId(row.getString(0));
            metadata.setIndex(row.getInt(1));
            metadata.setToken(row.getString(2));
            metadata.setStatus(SchemaMigrationStatus.valueOf(row.getString(3)));
            metadata.setFingerprint(row.getString(4));
            return metadata;
        }).one();
    }

    @Override
    public void update(SchemaMigrationMetadata metadata) {
        session.execute(
                insertInto(METADATA_TABLE)
                        .value(ID_COL, metadata.getId())
                        .value(INDEX_COL, metadata.getIndex())
                        .value(TOKEN_COL, metadata.getToken())
                        .value(STATUS_COL, metadata.getStatus().name())
                        .value(FINGERPRINT_COL, metadata.getFingerprint()));
    }
}
