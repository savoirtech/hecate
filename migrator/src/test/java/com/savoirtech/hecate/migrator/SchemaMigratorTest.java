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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.savoirtech.hecate.migrator.exception.SchemaMigrationException;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SchemaMigratorTest extends CassandraTestCase {

    @Mock
    private SchemaMigration step1;

    @Mock
    private SchemaMigration step2;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
        SchemaMigrationMetadataRepositoryImpl.createMigrationTable(getSession());
    }

    @Test(expected = SchemaMigrationException.class)
    public void testWithNoMigrationTable() {
        getSession().execute(SchemaBuilder.dropTable(SchemaMigrationMetadataRepositoryImpl.METADATA_TABLE));
        new SchemaMigrator().execute(new SchemaMigrationMetadataRepositoryImpl(getSession()));
    }


    @Test(expected = SchemaMigrationException.class)
    public void testWithMigrationsOutOfSync() {
        SchemaMigrationMetadataRepository repository = new SchemaMigrationMetadataRepositoryImpl(getSession());
        repository.update(new SchemaMigrationMetadata(new SchemaMigrationDescriptor("foo", 1, step1)));
        new SchemaMigrator().addMigration("foo", step1).execute(repository);
    }

    @Test
    public void testExecutingAnotherProcessRunningAbort() {
        SchemaMigrationMetadataRepository repository = new SchemaMigrationMetadataRepositoryImpl(getSession());
        repository.update(new SchemaMigrationMetadata(new SchemaMigrationDescriptor("Mock Migration", 0, step1)));
        new SchemaMigrator().addMigration("Mock Migration", step1).execute(repository);
        verifyNoMoreInteractions(step1);
    }

    @Test
    public void testExecutingAfterCompletedStep() {
        SchemaMigrationMetadataRepository repository = new SchemaMigrationMetadataRepositoryImpl(getSession());
        SchemaMigrationMetadata step1Metadata = new SchemaMigrationMetadata(new SchemaMigrationDescriptor("Completed Migration", 0, step1));
        step1Metadata.setStatus(SchemaMigrationStatus.Complete);
        repository.update(step1Metadata);
        new SchemaMigrator().addMigration("Completed Migration", step1).addMigration("Mock Migration", step2).execute(repository);
        verify(step2).execute(getSession());
        verifyNoMoreInteractions(step1);
    }

    @Test
    public void testExecutingFingerprintMismatchAbort() {
        SchemaMigrationMetadataRepository repository = new SchemaMigrationMetadataRepositoryImpl(getSession());
        SchemaMigrationMetadata metadata = new SchemaMigrationMetadata(new SchemaMigrationDescriptor("Mock Migration", 0, new BogusMigration()));
        metadata.setStatus(SchemaMigrationStatus.Complete);
        repository.update(metadata);
        new SchemaMigrator().addMigration("Mock Migration", step1).execute(repository);
        verifyNoMoreInteractions(step1);
    }

    private static class BogusMigration implements SchemaMigration {
        @Override
        public void execute(Session session) {
            // Do nothing!
        }
    }
}