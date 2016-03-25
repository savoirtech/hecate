/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.dao;

import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Test;

public class WrapperObjectsTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    @Cassandra
    public void testNullsPreserved() {
        PojoDao<WrapperPojo> dao = createPojoDao(WrapperPojo.class);
        WrapperPojo pojo = new WrapperPojo();
        dao.save(pojo);

        WrapperPojo found = dao.findByKeys(pojo.getId());
        assertNull(found.getBooleanWrapper());
        assertNull(found.getDoubleWrapper());
        assertNull(found.getFloatWrapper());
        assertNull(found.getIntWrapper());
        assertNull(found.getLongWrapper());
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class WrapperPojo  extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private Integer intWrapper;
        private Long longWrapper;
        private Boolean booleanWrapper;
        private Double doubleWrapper;
        private Float floatWrapper;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public Boolean getBooleanWrapper() {
            return booleanWrapper;
        }

        public void setBooleanWrapper(Boolean booleanWrapper) {
            this.booleanWrapper = booleanWrapper;
        }

        public Double getDoubleWrapper() {
            return doubleWrapper;
        }

        public void setDoubleWrapper(Double doubleWrapper) {
            this.doubleWrapper = doubleWrapper;
        }

        public Float getFloatWrapper() {
            return floatWrapper;
        }

        public void setFloatWrapper(Float floatWrapper) {
            this.floatWrapper = floatWrapper;
        }

        public Integer getIntWrapper() {
            return intWrapper;
        }

        public void setIntWrapper(Integer intWrapper) {
            this.intWrapper = intWrapper;
        }

        public Long getLongWrapper() {
            return longWrapper;
        }

        public void setLongWrapper(Long longWrapper) {
            this.longWrapper = longWrapper;
        }
    }
}
