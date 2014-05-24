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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.savoirtech.hecate.core.annotations.CFName;
import com.savoirtech.hecate.core.annotations.CassandraId;

public class Top {

    @CassandraId
    String id;

    @CFName(name = "LOOL")
    Child child;

    @CFName(name = "BOB")
    List<Child> children = new ArrayList<>();
    @CFName(name = "BOB2")

    List<Child> moreKids = new ArrayList<>();

    @CFName(name = "CHILDSET")
    Set<Child> childSet = new HashSet<>();

    @CFName(name = "CHILDMAP")
    Map<String, Child> childMap = new HashMap<>();

    Map<String, String> bobs = new HashMap<>();

    List<String> kidIds = new ArrayList<>();

    public List<Child> getMoreKids() {
        return moreKids;
    }

    public void setMoreKids(List<Child> moreKids) {
        this.moreKids = moreKids;
    }

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

    public Set<Child> getChildSet() {
        return childSet;
    }

    public void setChildSet(Set<Child> childSet) {
        this.childSet = childSet;
    }

    public Map<String, Child> getChildMap() {
        return childMap;
    }

    public void setChildMap(Map<String, Child> childMap) {
        this.childMap = childMap;
    }

    public List<String> getKidIds() {
        return kidIds;
    }

    public void setKidIds(List<String> kidIds) {
        this.kidIds = kidIds;
    }

    public Map<String, String> getBobs() {
        return bobs;
    }

    public void setBobs(Map<String, String> bobs) {
        this.bobs = bobs;
    }

    @Override
    public String toString() {
        return "Top{" +
            "id='" + id + '\'' +
            ", children=" + children +
            ", moreKids=" + moreKids +
            ", childSet=" + childSet +
            ", childMap=" + childMap +
            ", kidIds=" + kidIds +
            ", bobs=" + bobs +
            '}';
    }
}
