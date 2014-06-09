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

package com.savoirtech.hecate.cql3.entities;

import com.savoirtech.hecate.cql3.annotations.IdColumn;
import com.savoirtech.hecate.cql3.annotations.TableName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Parent {

    @IdColumn
    long id;

    @TableName("child")
    Child child;

    @TableName("childlist")
    List<Child> childList = new ArrayList<>();

    @TableName("childset")
    Set<Child> childSet = new HashSet<>();

    @TableName("childmap")
    Map<String, Child> childMap = new HashMap<>();

    @TableName("longchildmap")
    Map<Long, Child> longChildMap = new HashMap<>();

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Child getChild() {
        return child;
    }

    public void setChild(Child child) {
        this.child = child;
    }

    public List<Child> getChildList() {
        return childList;
    }

    public void setChildList(List<Child> childList) {
        this.childList = childList;
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

    public Map<Long, Child> getLongChildMap() {
        return longChildMap;
    }

    public void setLongChildMap(Map<Long, Child> longChildMap) {
        this.longChildMap = longChildMap;
    }

    @Override
    public String toString() {
        return "Parent{" +
                "id=" + id +
                ", child=" + child +
                ", childList=" + childList +
                ", childSet=" + childSet +
                ", childMap=" + childMap +
                ", longChildMap=" + longChildMap +
                '}';
    }
}
