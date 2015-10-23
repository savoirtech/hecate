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

import com.eaio.uuid.UUID;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.SerializationUtils;

public class SchemaMigrationMetadata {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private String id;
    private int index;
    private String token;
    private SchemaMigrationStatus status;
    private String fingerprint;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    protected static String fingerprint(SchemaMigrationDescriptor descriptor) {
        return DigestUtils.md5Hex(SerializationUtils.serialize(descriptor.getSchemaMigration()));
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SchemaMigrationMetadata() {
    }

    public SchemaMigrationMetadata(SchemaMigrationDescriptor descriptor) {
        this.id = descriptor.getId();
        this.index = descriptor.getIndex();
        this.token = new UUID().toString();
        this.fingerprint = fingerprint(descriptor);
        this.status = SchemaMigrationStatus.Running;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public SchemaMigrationStatus getStatus() {
        return status;
    }

    public void setStatus(SchemaMigrationStatus status) {
        this.status = status;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public boolean isRunning() {
        return SchemaMigrationStatus.Running.equals(status);
    }
}
