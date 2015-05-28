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

import com.savoirtech.hecate.annotation.Cascade;
import com.savoirtech.hecate.annotation.Id;

import java.util.UUID;

public class CascadedPojo {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    @Id
    private String id = UUID.randomUUID().toString();

    @Cascade(delete = false)
    private NestedPojo saveOnly;

    @Cascade(save = false)
    private NestedPojo deleteOnly;

    @Cascade(save = false, delete = false)
    private NestedPojo noCascade;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public NestedPojo getDeleteOnly() {
        return deleteOnly;
    }

    public void setDeleteOnly(NestedPojo deleteOnly) {
        this.deleteOnly = deleteOnly;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public NestedPojo getNoCascade() {
        return noCascade;
    }

    public void setNoCascade(NestedPojo noCascade) {
        this.noCascade = noCascade;
    }

    public NestedPojo getSaveOnly() {
        return saveOnly;
    }

    public void setSaveOnly(NestedPojo saveOnly) {
        this.saveOnly = saveOnly;
    }
}
