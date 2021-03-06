/*
 * Copyright (c) 2011-2017 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.test.view;


import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleObject;
import org.iternine.jeppetto.dao.test.core.DynamicDAO;

import org.junit.After;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;


public abstract class SummaryDAOTest {

    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract DynamicDAO getDynamicDAO();
    protected abstract SummaryDAO getSummaryDAO();

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
    public void findById()
            throws NoSuchItemException {
        RelatedObject relatedObject;
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(20);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setRelatedObject(relatedObject);       // Add item to object
        simpleObject.addRelatedObject(relatedObject);       // ...and to List

        getDynamicDAO().save(simpleObject);

        SummarySimpleObject summarySimpleObject = getSummaryDAO().findById(simpleObject.getId());

        assertEquals(simpleObject.getId(), summarySimpleObject.getId());
        assertEquals(simpleObject.getRelatedObject().getRelatedIntValue(), summarySimpleObject.getRelatedObject().getRelatedIntValue());
        assertEquals(1, summarySimpleObject.getRelatedObjects().size());
    }


    @Test
    public void updateSummaryObject() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        getDynamicDAO().save(simpleObject);

        SummarySimpleObject summarySimpleObject = getSummaryDAO().findById(simpleObject.getId());

        summarySimpleObject.setIntValue(999);

        getSummaryDAO().save(summarySimpleObject);

        SummarySimpleObject resultSummarySimpleObject = getSummaryDAO().findById(simpleObject.getId());

        assertEquals(999, resultSummarySimpleObject.getIntValue());

        SimpleObject resultSimpleObject = getDynamicDAO().findById(simpleObject.getId());

        assertEquals(999, resultSimpleObject.getIntValue());
    }


    @Test
    public void findMultiple() {
        Set<Integer> intValues = new HashSet<Integer>(10);

        for (int i = 0; i < 10; i++) {
            SimpleObject simpleObject = new SimpleObject();
            simpleObject.setIntValue(i);

            getDynamicDAO().save(simpleObject);

            intValues.add(i);
        }

        Iterable<SummarySimpleObject> results = getSummaryDAO().findAll();

        for (SummarySimpleObject result : results) {
            intValues.remove(result.getIntValue());
        }

        assertEquals(0, intValues.size());
    }


    @Test
    public void findMultipleById() {
        Set<String> ids = new HashSet<String>(3);

        for (int i = 0; i < 3; i++) {
            SimpleObject simpleObject = new SimpleObject();
            simpleObject.setIntValue(i);

            getDynamicDAO().save(simpleObject);

            ids.add(simpleObject.getId());
        }

        Iterable<SummarySimpleObject> results = getSummaryDAO().findByIds(ids.toArray(new String[3]));

        for (SummarySimpleObject result : results) {
            ids.remove(result.getId());
        }

        assertEquals(0, ids.size());
    }
}
