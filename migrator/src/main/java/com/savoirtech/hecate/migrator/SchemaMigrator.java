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

import java.util.ArrayList;
import java.util.List;

import com.savoirtech.hecate.migrator.exception.SchemaMigrationException;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaMigrator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);

    private final List<SchemaMigrationDescriptor> descriptors = new ArrayList<>();

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public SchemaMigrator addMigration(String id, SchemaMigration migration) {
        descriptors.add(new SchemaMigrationDescriptor(id, descriptors.size(), migration));
        return this;
    }

    public void execute(SchemaMigrationMetadataRepository repository) {
        for (SchemaMigrationDescriptor descriptor : descriptors) {
            final SchemaMigrationMetadata expected = new SchemaMigrationMetadata(descriptor);
            repository.create(expected);
            final SchemaMigrationMetadata actual = repository.retrieve(descriptor.getId());
            if (ObjectUtils.notEqual(expected.getIndex(), actual.getIndex())) {
                throw new SchemaMigrationException("Schema migrations out of sync.  Schema migration \"%s\" previously executed at position %d, but is currently at position %d.", descriptor.getId(), actual.getIndex(), expected.getIndex());
            }
            if (StringUtils.equals(expected.getToken(), actual.getToken())) {
                LOGGER.info("Executing schema migration \"{}\"...", actual.getId());
                descriptor.getSchemaMigration().execute(repository.getSession());
                actual.setStatus(SchemaMigrationStatus.Complete);
                repository.update(actual);
            } else {
                if (actual.isRunning()) {
                    LOGGER.error("Another process is currently executing schema migration \"{}\", aborting.", actual.getId());
                    return;
                } else if (!StringUtils.equals(expected.getFingerprint(), actual.getFingerprint())) {
                    LOGGER.error("Schema migration \"{}\" has changed since it was executed (fingerprint mismatch), aborting.", actual.getId());
                    return;
                }
                LOGGER.info("Schema migration \"{}\" already complete, continuing...", actual.getId());
            }
        }
    }
}
