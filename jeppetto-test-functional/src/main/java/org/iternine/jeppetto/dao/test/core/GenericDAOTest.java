/*
 * Copyright (c) 2011-2014 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.test.core;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleEnum;
import org.iternine.jeppetto.dao.test.SimpleObject;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


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
    public void findById()
            throws NoSuchItemException {
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
    public void saveAndFindWithBasicFields()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(5);
        simpleObject.setLongValue(100l);
        simpleObject.setSimpleEnum(SimpleEnum.EnumValue);

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
        assertEquals(5, resultObject.getIntValue());
        assertEquals(100l, resultObject.getLongValue());
        assertEquals(SimpleEnum.EnumValue, resultObject.getSimpleEnum());
    }


    @Test
    public void saveAndUpdateBasicFields()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(5);
        simpleObject.setLongValue(100l);
        simpleObject.setSimpleEnum(SimpleEnum.EnumValue);

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        resultObject.setIntValue(10);

        getGenericDAO().save(resultObject);

        resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
        assertEquals(10, resultObject.getIntValue());
        assertEquals(100l, resultObject.getLongValue());
        assertEquals(SimpleEnum.EnumValue, resultObject.getSimpleEnum());
    }


    @Test
    public void saveAndFindWithStringList()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setStringList(new ArrayList<String>());
        simpleObject.getStringList().add("foo");
        simpleObject.getStringList().add("bar");
        simpleObject.getStringList().add("bar");

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertNotNull(resultObject.getStringList());
        assertEquals(3, resultObject.getStringList().size());
        assertEquals("foo", resultObject.getStringList().get(0));
        assertEquals("bar", resultObject.getStringList().get(1));
        assertEquals("bar", resultObject.getStringList().get(2));
    }


    @Test
    public void saveAndFindWithStringSet()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setStringSet(new HashSet<String>());
        simpleObject.getStringSet().add("foo");
        simpleObject.getStringSet().add("bar");
        simpleObject.getStringSet().add("bar");

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertNotNull(resultObject.getStringSet());
        assertEquals(2, resultObject.getStringSet().size());
        assertTrue(resultObject.getStringSet().contains("foo"));
        assertTrue(resultObject.getStringSet().contains("bar"));
    }


    @Test
    public void saveAndFindWithStringMap()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setStringMap(new HashMap<String, String>());
        simpleObject.getStringMap().put("foo", "bar");

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertNotNull(resultObject.getStringMap());
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
        assertTrue(simpleObject.getRelatedObjectMap().containsKey("foo"));
        assertTrue(simpleObject.getRelatedObjectMap().containsKey("bar"));
        assertEquals(simpleObject.getRelatedObjectMap().get("foo").getRelatedIntValue(),
                     simpleObject.getRelatedObjectMap().get("bar").getRelatedIntValue());
    }


    @Test
    public void addToListAcrossSaves() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setId("id");
        simpleObject.setRelatedObjects(new ArrayList<RelatedObject>());

        RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(123);
        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(345);

        simpleObject.addRelatedObject(relatedObject1);
        getGenericDAO().save(simpleObject);

        SimpleObject result1 = getGenericDAO().findById("id");
        result1.addRelatedObject(relatedObject2);
        getGenericDAO().save(result1);

        SimpleObject result2 = getGenericDAO().findById("id");
        result2.addRelatedObject(relatedObject2);
        getGenericDAO().save(result2);

        SimpleObject finalResult= getGenericDAO().findById("id");
        assertEquals(3, finalResult.getRelatedObjects().size());
        assertEquals(123, finalResult.getRelatedObjects().get(0).getRelatedIntValue());
        assertEquals(345, finalResult.getRelatedObjects().get(1).getRelatedIntValue());
        assertEquals(345, finalResult.getRelatedObjects().get(2).getRelatedIntValue());
    }


    @Test
    public void addToListUsingIndexAcrossSaves() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setId("id");
        simpleObject.setRelatedObjects(new ArrayList<RelatedObject>());

        RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(123);
        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(345);

        simpleObject.addRelatedObject(relatedObject1);
        getGenericDAO().save(simpleObject);

        SimpleObject result1 = getGenericDAO().findById("id");
        result1.getRelatedObjects().add(0, relatedObject2);
        getGenericDAO().save(result1);

        SimpleObject finalResult= getGenericDAO().findById("id");
        assertEquals(2, finalResult.getRelatedObjects().size());
        assertEquals(345, finalResult.getRelatedObjects().get(0).getRelatedIntValue());
        assertEquals(123, finalResult.getRelatedObjects().get(1).getRelatedIntValue());
    }


    @Test
    public void addToMapAcrossSaves() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setId("id");
        simpleObject.setRelatedObjectMap(new HashMap<String, RelatedObject>());

        RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(123);
        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(345);

        simpleObject.getRelatedObjectMap().put("first", relatedObject1);
        getGenericDAO().save(simpleObject);

        SimpleObject result1 = getGenericDAO().findById("id");
        result1.getRelatedObjectMap().put("second", relatedObject2);
        getGenericDAO().save(result1);

        SimpleObject result2 = getGenericDAO().findById("id");
        result2.getRelatedObjectMap().put("third", relatedObject2);
        getGenericDAO().save(result2);

        SimpleObject finalResult= getGenericDAO().findById("id");
        assertEquals(123, finalResult.getRelatedObjectMap().get("first").getRelatedIntValue());
        assertEquals(345, finalResult.getRelatedObjectMap().get("second").getRelatedIntValue());
        assertEquals(345, finalResult.getRelatedObjectMap().get("third").getRelatedIntValue());
    }


    @Test
    public void saveAndUpdateComplexObject()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(5678);

        simpleObject.setRelatedObject(relatedObject);

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        resultObject.getRelatedObject().setRelatedIntValue(1111);

        getGenericDAO().save(resultObject);

        resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
        assertEquals(1111, resultObject.getRelatedObject().getRelatedIntValue());
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

        assertTrue(resultCount == 2);

        // check for existence of objects?
    }


    @Test
    public void transientValuesAreNotPersisted()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(5);
        simpleObject.setTransientValue(10);

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
        assertEquals(5, resultObject.getIntValue());
        assertEquals(0, resultObject.getTransientValue());
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
