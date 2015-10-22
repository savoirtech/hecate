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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.migrator.exception.SchemaMigrationException;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;

public class SchemaMigration {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final String MIGRATION_TABLE = "hecate_migration";
    public static final String ID_COL = "id";
    public static final String INDEX_COL = "ndx";
    public static final String TOKEN_COL = "sync_token";
    public static final String STATUS_COL = "status";
    public static final String FINGERPRINT_COL = "fingerprint";
    private static Logger LOGGER = LoggerFactory.getLogger(SchemaMigration.class);

    private List<IdAndStep> agenda = new ArrayList<>();

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static void createMigrationTable(Session session) {
        session.execute(createTable(MIGRATION_TABLE)
                .addPartitionKey(ID_COL, varchar())
                .addColumn(INDEX_COL, cint())
                .addColumn(TOKEN_COL, varchar())
                .addColumn(STATUS_COL, varchar())
                .addColumn(FINGERPRINT_COL, varchar()));
    }

    protected static String fingerprint(IdAndStep idAndStep) {
        try {
            try (ByteArrayOutputStream bout = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bout)) {
                ObjectStreamClass osc = ObjectStreamClass.lookup(idAndStep.getStep().getClass());
                out.writeLong(osc.getSerialVersionUID());
                out.writeObject(idAndStep.getStep());
                out.close();
                return DigestUtils.md5Hex(bout.toByteArray());
            }
        } catch (IOException e) {
            throw new SchemaMigrationException(e, "Unable to calculate fingerprint for migration step \"{}\".", idAndStep.getId());
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public SchemaMigration addStep(String id, SchemaMigrationStep step) {
        agenda.add(new IdAndStep(id, step));
        return this;
    }

    public void execute(Session session) {
        session.getCluster().getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
        if (keyspace.getTable(MIGRATION_TABLE) == null) {
            throw new SchemaMigrationException("Schema migration table \"%s\" not found.", MIGRATION_TABLE);
        }

        int currentIndex = 0;
        for (IdAndStep idAndStep : agenda) {
            final String fingerprint = fingerprint(idAndStep);
            SchemaMigrationStepTracking tracking = getStepTracking(idAndStep.getId(), session);
            if (tracking != null) {
                if (currentIndex != tracking.getIndex()) {
                    throw new SchemaMigrationException("Schema migration step \"%s\" is out of sync.  Expected index %d, but was %d.", idAndStep.getId(), currentIndex, tracking.getIndex());
                } else if (requiresAbort(tracking, fingerprint)) {
                    return;
                }
            } else {
                String syncToken = newSyncToken();
                tracking = new SchemaMigrationStepTracking();
                tracking.setId(idAndStep.getId());
                tracking.setIndex(currentIndex);
                tracking.setToken(syncToken);
                tracking.setStatus(SchemaMigrationStepStatus.Running);
                tracking.setFingerprint(fingerprint);
                createStepTracking(tracking, session);
                tracking = getStepTracking(idAndStep.getId(), session);
                if (syncToken.equals(tracking.getToken())) {
                    LOGGER.info("Executing migration step \"{}\"...", tracking.getId());
                    idAndStep.getStep().execute(session);
                    tracking.setStatus(SchemaMigrationStepStatus.Complete);
                    updateStepTracking(tracking, session);
                } else if (requiresAbort(tracking, fingerprint)) {
                    return;
                }
            }
            currentIndex++;
        }
    }

    private void createStepTracking(SchemaMigrationStepTracking tracking, Session session) {
        session.execute(
                insertInto(MIGRATION_TABLE)
                        .ifNotExists()
                        .value(ID_COL, tracking.getId())
                        .value(INDEX_COL, tracking.getIndex())
                        .value(TOKEN_COL, tracking.getToken())
                        .value(STATUS_COL, tracking.getStatus().name())
                        .value(FINGERPRINT_COL, tracking.getFingerprint()));
    }

    private SchemaMigrationStepTracking getStepTracking(String id, Session session) {
        ResultSet resultSet = session.execute(select(ID_COL, INDEX_COL, TOKEN_COL, STATUS_COL, FINGERPRINT_COL).from(MIGRATION_TABLE).where(eq(ID_COL, id)));
        return new MappedQueryResult<>(resultSet, row -> {
            SchemaMigrationStepTracking tracking = new SchemaMigrationStepTracking();
            tracking.setId(row.getString(0));
            tracking.setIndex(row.getInt(1));
            tracking.setToken(row.getString(2));
            tracking.setStatus(SchemaMigrationStepStatus.valueOf(row.getString(3)));
            tracking.setFingerprint(row.getString(4));
            return tracking;
        }).one();
    }

    private void updateStepTracking(SchemaMigrationStepTracking tracking, Session session) {
        session.execute(
                insertInto(MIGRATION_TABLE)
                        .value(ID_COL, tracking.getId())
                        .value(INDEX_COL, tracking.getIndex())
                        .value(TOKEN_COL, tracking.getToken())
                        .value(STATUS_COL, tracking.getStatus().name())
                        .value(FINGERPRINT_COL, tracking.getFingerprint()));
    }

    private boolean requiresAbort(SchemaMigrationStepTracking existing, String expectedFingerprint) {
        if(SchemaMigrationStepStatus.Running.equals(existing.getStatus())) {
            LOGGER.error("Another process is current executing migration step \"{}\", aborting.", existing.getId());
            return true;
        }
        
        if (!StringUtils.equals(expectedFingerprint, existing.getFingerprint())) {
            LOGGER.error("Fingerprint for migration step \"{}\" does not match expected value, aborting.", existing.getId());
            return true;
        }

        LOGGER.info("Migration step \"{}\" already complete, continuing...", existing.getId());
        return false;
    }

    protected String newSyncToken() {
        return UUID.randomUUID().toString();
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    protected static class IdAndStep {
        private final String id;
        private final SchemaMigrationStep step;

        public IdAndStep(String id, SchemaMigrationStep step) {
            this.id = id;
            this.step = step;
        }

        public String getId() {
            return id;
        }

        public SchemaMigrationStep getStep() {
            return step;
        }
    }
}
