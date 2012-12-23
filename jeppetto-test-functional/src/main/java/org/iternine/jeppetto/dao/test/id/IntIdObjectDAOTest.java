/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.iternine.jeppetto.dao.test.id;


import org.iternine.jeppetto.dao.NoSuchItemException;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public abstract class IntIdObjectDAOTest {

    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract IntIdObjectDAO getIntIdObjectDAO();

    protected abstract void reset();


    //-------------------------------------------------------------
    // Methods - Test Lifecycle
    //-------------------------------------------------------------

    @After
    public void after() {
        reset();
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void manuallyManageId()
            throws NoSuchItemException {
        IntIdObject intIdObject = new IntIdObject();
        intIdObject.setId(1);
        intIdObject.setValue("value1");

        getIntIdObjectDAO().save(intIdObject);

        assertNotNull(intIdObject.getId());

        IntIdObject resultObject = getIntIdObjectDAO().findById(1);

        assertEquals(resultObject.getId(), intIdObject.getId());
    }
}
