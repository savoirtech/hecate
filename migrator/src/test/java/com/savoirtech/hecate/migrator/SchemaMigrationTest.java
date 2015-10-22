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

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.savoirtech.hecate.migrator.exception.SchemaMigrationException;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.savoirtech.hecate.migrator.SchemaMigration.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SchemaMigrationTest extends CassandraTestCase {

    @Mock
    private SchemaMigrationStep step1;

    @Mock
    private SchemaMigrationStep step2;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(expected = SchemaMigrationException.class)
    public void testWithNoMigrationTable() {
        new SchemaMigration().execute(getSession());
    }

    @Test(expected = SchemaMigrationException.class)
    public void testWithOutOfSyncStep() {
        SchemaMigration.createMigrationTable(getSession());
        getSession().execute(QueryBuilder
                .insertInto(MIGRATION_TABLE)
                .value(ID_COL, "foo")
                .value(SchemaMigration.INDEX_COL, 1)
                .value(TOKEN_COL, "token")
                .value(STATUS_COL, SchemaMigrationStepStatus.Complete.name()));
        new SchemaMigration().addStep("foo", step1).execute(getSession());
    }

    @Test
    public void testExecutingAnotherProcessRunningAbort() {
        SchemaMigration.createMigrationTable(getSession());
        getSession().execute(QueryBuilder
                .insertInto(MIGRATION_TABLE)
                .value(ID_COL, "Mock Step")
                .value(SchemaMigration.INDEX_COL, 0)
                .value(TOKEN_COL, "token")
                .value(STATUS_COL, SchemaMigrationStepStatus.Running.name())
                .value(FINGERPRINT_COL, SchemaMigration.fingerprint(new IdAndStep("Completed Step", step1))));
        new SchemaMigration().addStep("Mock Step", step1).execute(getSession());
        verifyNoMoreInteractions(step1);
    }

    @Test
    public void testExecutingAfterCompletedStep() {
        SchemaMigration.createMigrationTable(getSession());
        getSession().execute(QueryBuilder
                .insertInto(MIGRATION_TABLE)
                .value(ID_COL, "Completed Step")
                .value(SchemaMigration.INDEX_COL, 0)
                .value(TOKEN_COL, "token")
                .value(STATUS_COL, SchemaMigrationStepStatus.Complete.name())
                .value(FINGERPRINT_COL, SchemaMigration.fingerprint(new IdAndStep("Completed Step", step1))));
        new SchemaMigration().addStep("Completed Step", step1).addStep("Mock Step", step2).execute(getSession());
        verify(step2).execute(getSession());
        verifyNoMoreInteractions(step1);
    }

    @Test
    public void testExecutingFingerprintMismatchAbort() {
        SchemaMigration.createMigrationTable(getSession());
        getSession().execute(QueryBuilder
                .insertInto(MIGRATION_TABLE)
                .value(ID_COL, "Mock Step")
                .value(SchemaMigration.INDEX_COL, 0)
                .value(TOKEN_COL, "token")
                .value(STATUS_COL, SchemaMigrationStepStatus.Complete.name())
                .value(FINGERPRINT_COL, "bogusFingerprint"));
        new SchemaMigration().addStep("Mock Step", step1).execute(getSession());
        verifyNoMoreInteractions(step1);
    }

    @Test
    public void testExecutingStep() {
        SchemaMigration.createMigrationTable(getSession());
        new SchemaMigration().addStep("Mock Step", step1).execute(getSession());
        verify(step1).execute(getSession());
    }

}