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

public class SchemaMigrationDescriptor {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final String id;
    private final int index;
    private final SchemaMigration schemaMigration;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SchemaMigrationDescriptor(String id, int index, SchemaMigration schemaMigration) {
        this.id = id;
        this.index = index;
        this.schemaMigration = schemaMigration;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public String getId() {
        return id;
    }

    public int getIndex() {
        return index;
    }

    public SchemaMigration getSchemaMigration() {
        return schemaMigration;
    }
}
