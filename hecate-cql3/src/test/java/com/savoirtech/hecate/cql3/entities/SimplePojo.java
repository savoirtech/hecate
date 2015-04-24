/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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

import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.annotations.Index;
import com.savoirtech.hecate.cql3.annotations.Table;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Table(name = "simpletons")
public class SimplePojo {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    @Id
    private String id = UUID.randomUUID().toString();

    @Index(name = "name_ndx")
    private String name;

    private int[] ints;

    private byte[] bytes;

    private ByteBuffer byteBuffer;

    private List<String> listOfStrings;

    private Set<String> setOfStrings;

    private Map<Integer, String> mapOfStrings;

    @Index
    private Nums nums;

    private NestedPojo[] pojoArray;

    private NestedPojo nestedPojo;

    private List<NestedPojo> pojoList;

    private Set<NestedPojo> pojoSet;

    private Map<String, NestedPojo> pojoMap;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int[] getInts() {
        return ints;
    }

    public void setInts(int[] ints) {
        this.ints = ints;
    }

    public List<String> getListOfStrings() {
        return listOfStrings;
    }

    public void setListOfStrings(List<String> listOfStrings) {
        this.listOfStrings = listOfStrings;
    }

    public Map<Integer, String> getMapOfStrings() {
        return mapOfStrings;
    }

    public void setMapOfStrings(Map<Integer, String> mapOfStrings) {
        this.mapOfStrings = mapOfStrings;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public NestedPojo getNestedPojo() {
        return nestedPojo;
    }

    public void setNestedPojo(NestedPojo nestedPojo) {
        this.nestedPojo = nestedPojo;
    }

    public Nums getNums() {
        return nums;
    }

    public void setNums(Nums nums) {
        this.nums = nums;
    }

    public NestedPojo[] getPojoArray() {
        return pojoArray;
    }

    public void setPojoArray(NestedPojo[] pojoArray) {
        this.pojoArray = pojoArray;
    }

    public List<NestedPojo> getPojoList() {
        return pojoList;
    }

    public void setPojoList(List<NestedPojo> pojoList) {
        this.pojoList = pojoList;
    }

    public Map<String, NestedPojo> getPojoMap() {
        return pojoMap;
    }

    public void setPojoMap(Map<String, NestedPojo> pojoMap) {
        this.pojoMap = pojoMap;
    }

    public Set<NestedPojo> getPojoSet() {
        return pojoSet;
    }

    public void setPojoSet(Set<NestedPojo> pojoSet) {
        this.pojoSet = pojoSet;
    }

    public Set<String> getSetOfStrings() {
        return setOfStrings;
    }

    public void setSetOfStrings(Set<String> setOfStrings) {
        this.setOfStrings = setOfStrings;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer buff) {
        this.byteBuffer = buff;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimplePojo that = (SimplePojo) o;

        if (!id.equals(that.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static enum Nums {
        One, Two, Three
    }
}
