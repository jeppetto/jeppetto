/*
 * Copyright (c) 2011-2012 Jeppetto and Jonathan Thompson
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
import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleObject;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;


public abstract class GenericDAOTest {

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

    @Test
    public void findById() throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void findByBogusId()
            throws NoSuchItemException {
        getGenericDAO().findById("bogusId");
    }


    @Test
    public void saveAndFindWithMapOfStringToString() throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setStringMap(new HashMap<String, String>());
        simpleObject.getStringMap().put("foo", "bar");

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals("bar", resultObject.getStringMap().get("foo"));
    }


    @Test
    public void saveAndFindComplexObject()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(5678);

        simpleObject.setRelatedObject(relatedObject);
        simpleObject.addRelatedObject(relatedObject);

        simpleObject.addRelatedObject("foo", relatedObject);
        simpleObject.addRelatedObject("bar", relatedObject);

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
        RelatedObject[] objects = resultObject.getRelatedObjects().toArray(new RelatedObject[1]);
        assertEquals(relatedObject.getRelatedIntValue(), objects[0].getRelatedIntValue());
        assertEquals(relatedObject.getRelatedIntValue(), resultObject.getRelatedObject().getRelatedIntValue());
        Assert.assertTrue(simpleObject.getRelatedObjectMap().containsKey("foo"));
        Assert.assertTrue(simpleObject.getRelatedObjectMap().containsKey("bar"));
        assertEquals(simpleObject.getRelatedObjectMap().get("foo").getRelatedIntValue(),
                     simpleObject.getRelatedObjectMap().get("bar").getRelatedIntValue());
    }


    @Test(expected = NoSuchItemException.class)
    public void saveAndDelete()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());

        getGenericDAO().delete(simpleObject);

        getGenericDAO().findById(simpleObject.getId());
    }


    @Test
    public void saveMultipleAndFindAll()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        getGenericDAO().save(simpleObject);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject2.setIntValue(5678);

        getGenericDAO().save(simpleObject2);

        Iterable<SimpleObject> results = getGenericDAO().findAll();

        int resultCount = 0;
        // noinspection UnusedDeclaration
        for (SimpleObject ignore : results) {
            resultCount++;
        }

        Assert.assertTrue(resultCount >= 2);

        // check for existence of objects?
    }


    @Test(expected = JeppettoException.class)
    public void idReuseShouldFail() {
        SimpleObject simpleObject1 = new SimpleObject();
        simpleObject1.setId("a");
        simpleObject1.setIntValue(-1);
        getGenericDAO().save(simpleObject1);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject2.setId("a");
        simpleObject2.setIntValue(3);
        getGenericDAO().save(simpleObject2);
    }


    @Test(expected = JeppettoException.class)
    public void uniqueConstraintCausesException() {
        SimpleObject simpleObject1 = new SimpleObject();
        simpleObject1.setIntValue(123);
        getGenericDAO().save(simpleObject1);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject2.setIntValue(123);
        getGenericDAO().save(simpleObject2);
    }
}
