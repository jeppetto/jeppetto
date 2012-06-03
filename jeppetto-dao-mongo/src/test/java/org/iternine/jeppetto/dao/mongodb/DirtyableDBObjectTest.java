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

package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObject;
import org.iternine.jeppetto.test.RelatedObject;
import org.iternine.jeppetto.test.SimpleObject;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class DirtyableDBObjectTest {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private TestContext testContext;
    private GenericDAO<SimpleObject, String> simpleObjectDAO;


    //-------------------------------------------------------------
    // Methods - Set-Up / Tear-Down
    //-------------------------------------------------------------

    @Before
    public void setUp() {
        testContext = new TestContext("MongoDAOTest.spring.xml",
                                      "MongoDAOTest.properties",
                                      new MongoDatabaseProvider(false));

        //noinspection unchecked
        simpleObjectDAO = (GenericDAO<SimpleObject, String>) testContext.getBean("simpleObjectMongoQueryModelDAO");
    }


    @After
    public void tearDown() {
        if (testContext != null) {
            testContext.close();
        }
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void testEnhancedIsNotInitiallyDirty()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        // The object is not dirty if it isn't modified after it is enhanced
        Assert.assertFalse(((DirtyableDBObject) resultSimpleObject).isDirty());
    }


    @Test
    public void testEnhancedBecomesDirty()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());
        resultSimpleObject.setIntValue(2345);

        // The object is dirty if modified after it is enhanced
        Assert.assertTrue(((DirtyableDBObject) resultSimpleObject).isDirty());
    }


    @Test
    public void testModifySimpleFields()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());
        resultSimpleObject.setIntValue(9876);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("intValue")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(9876, simpleObjectDAO.findById(simpleObject.getId()).getIntValue());
    }


    @Test
    public void testSetRelatedObject()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        resultSimpleObject.setRelatedObject(relatedObject);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObject")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(999, simpleObjectDAO.findById(simpleObject.getId()).getRelatedObject().getRelatedIntValue());
    }


    @Test
    public void testUpdatedRelatedObject()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setRelatedObject(relatedObject);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        resultSimpleObject.getRelatedObject().setRelatedIntValue(123);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObject")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(123, simpleObjectDAO.findById(simpleObject.getId()).getRelatedObject().getRelatedIntValue());
    }


    @Test
    public void testReplaceRelatedObject()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setRelatedObject(relatedObject);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(123);

        resultSimpleObject.setRelatedObject(relatedObject2);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObject")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(123, simpleObjectDAO.findById(simpleObject.getId()).getRelatedObject().getRelatedIntValue());
    }


    @Test
    public void testAddRelatedObjectToList()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.addRelatedObject(relatedObject);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(123);

        resultSimpleObject.addRelatedObject(relatedObject2);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObjects")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(2, simpleObjectDAO.findById(simpleObject.getId()).getRelatedObjects().size());
    }


    @Test
    public void testUpdateRelatedObjectInList()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.addRelatedObject(relatedObject);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());
        RelatedObject resultRelatedObject = resultSimpleObject.getRelatedObjects().get(0);

        resultRelatedObject.setRelatedIntValue(123);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObjects")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(123, simpleObjectDAO.findById(simpleObject.getId()).getRelatedObjects().get(0).getRelatedIntValue());
    }


    @Test
    public void testModifyAndUpdateRelatedObjectList()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.addRelatedObject(relatedObject);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());
        RelatedObject resultRelatedObject = resultSimpleObject.getRelatedObjects().get(0);

        resultRelatedObject.setRelatedIntValue(123);

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(456);

        resultSimpleObject.addRelatedObject(relatedObject2);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObjects")));

        simpleObjectDAO.save(resultSimpleObject);

        List<RelatedObject> relatedObjectList = simpleObjectDAO.findById(simpleObject.getId()).getRelatedObjects();

        Assert.assertEquals(2, relatedObjectList.size());
        Assert.assertEquals(123, relatedObjectList.get(0).getRelatedIntValue());
        Assert.assertEquals(456, relatedObjectList.get(1).getRelatedIntValue());
    }


//    @Test
//    public void test2() {
//        SimpleObject enhancedSimpleObject = simpleObjectEnhancer.newInstance();
//        enhancedSimpleObject.addToRelatedObjectSet(new RelatedObject());
//
//        Assert.assertTrue(((DirtyableDBObject) enhancedSimpleObject).isDirty());
//    }


//    @SuppressWarnings({ "unchecked" })
//    @Test
//    public void test3() {
//        RelatedObject enhancedRelatedObject = relatedObjectEnhancer.newInstance();
//        enhancedRelatedObject.setRelatedIntValue(999);
//
//        Set relatedObjectSet = new DirtyableDBObjectSet();
//        relatedObjectSet.add(enhancedRelatedObject);
//
//        SimpleObject enhancedSimpleObject = simpleObjectEnhancer.newInstance();
//        enhancedSimpleObject.setRelatedObjectSet(relatedObjectSet);
//
//        RelatedObject relatedObject = enhancedSimpleObject.getRelatedObjectSet().iterator().next();
//
//        Assert.assertTrue(((DirtyableDBObject) enhancedSimpleObject).isDirty());
//    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void assertKeys(Iterator iterator, Set objects) {
        while (iterator.hasNext()) {
            Assert.assertTrue(objects.remove(iterator.next()));
        }

        Assert.assertTrue(objects.isEmpty());
    }
}
