/*
 * Copyright (c) 2011-2013 Jeppetto and Jonathan Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.iternine.jeppetto.dao.test.core;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.OptimisticLockException;
import org.iternine.jeppetto.dao.test.SimpleObject;

import org.junit.After;
import org.junit.Test;


public abstract class OptimisticLockTest {

    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract GenericDAO<SimpleObject, String> getGenericDAO();

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

    @Test(expected = OptimisticLockException.class)
    public void optimisticLockException() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setId("a");
        getGenericDAO().save(simpleObject);

        SimpleObject result1 = getGenericDAO().findById("a");
        SimpleObject result2 = getGenericDAO().findById("a");

        result1.setIntValue(1);
        result2.setIntValue(2);

        getGenericDAO().save(result1);
        getGenericDAO().save(result2);
    }
}
