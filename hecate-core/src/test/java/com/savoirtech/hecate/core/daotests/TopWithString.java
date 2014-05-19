/*
 * Copyright 2014 Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.core.daotests;

import java.util.ArrayList;
import java.util.List;

import com.savoirtech.hecate.core.annotations.CFName;
import com.savoirtech.hecate.core.annotations.CassandraId;

public class TopWithString {

    @CassandraId
    String id;

    @CFName(name = "BOB")
    List<Child> children = new ArrayList<>();
    @CFName(name = "BOB2")
    List<Child> moreKids = new ArrayList<>();

    List<String> strings = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Child> getChildren() {
        return children;
    }

    public void setChildren(List<Child> children) {
        this.children = children;
    }

    public List<Child> getMoreKids() {
        return moreKids;
    }

    public void setMoreKids(List<Child> moreKids) {
        this.moreKids = moreKids;
    }

    public List<String> getStrings() {
        return strings;
    }

    public void setStrings(List<String> strings) {
        this.strings = strings;
    }
}
