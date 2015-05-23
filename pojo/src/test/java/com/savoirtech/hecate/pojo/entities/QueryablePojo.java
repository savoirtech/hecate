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

package com.savoirtech.hecate.pojo.entities;

import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Id;
import com.savoirtech.hecate.annotation.PartitionKey;

import java.util.UUID;

public class QueryablePojo {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    @Id
    private Key key;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public QueryablePojo() {
    }

    public QueryablePojo(int value) {
        this.key = new Key(value);
    }

    public QueryablePojo(UUID partition, int value) {
        this.key = new Key(partition, value);
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public Key getKey() {
        return key;
    }

    public void setKey(Key key) {
        this.key = key;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class Key {
        @PartitionKey
        private UUID partition = UUID.randomUUID();

        @ClusteringColumn
        private int value;

        public Key(UUID partition, int value) {
            this.partition = partition;
            this.value = value;
        }

        public Key(int value) {
            this.value = value;
        }

        public Key() {
        }

        public UUID getPartition() {
            return partition;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }
}
