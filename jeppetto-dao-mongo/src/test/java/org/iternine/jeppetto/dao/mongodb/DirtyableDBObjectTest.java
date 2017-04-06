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

package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObject;
import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleObject;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
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
        testContext = new TestContext("MongoGenericDAOTest.spring.xml",
                                      "MongoDAOTest.properties",
                                      new MongoDatabaseProvider(false));

        //noinspection unchecked
        simpleObjectDAO = (GenericDAO<SimpleObject, String>) testContext.getBean("mongoGenericDAO");
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
    public void testDeleteRelatedObjectFromList()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(123);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.addRelatedObject(relatedObject);
        simpleObject.addRelatedObject(relatedObject2);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        resultSimpleObject.getRelatedObjects().remove(0);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObjects")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(1, simpleObjectDAO.findById(simpleObject.getId()).getRelatedObjects().size());
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
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton(
                "relatedObjects")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(123, simpleObjectDAO.findById(simpleObject.getId()).getRelatedObjects().get(
                0).getRelatedIntValue());
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


    @Test
    public void testRemoveFromIterator()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(123);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.addRelatedObject(relatedObject);
        simpleObject.addRelatedObject(relatedObject2);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        int iteratedItemCount = 0;
        for (Iterator<RelatedObject> roIterator = resultSimpleObject.getRelatedObjects().iterator(); roIterator.hasNext(); ) {
            iteratedItemCount++;
            RelatedObject ro = roIterator.next();

            if (ro.getRelatedIntValue() > 500) {
                roIterator.remove();
            }
        }

        Assert.assertEquals(2, iteratedItemCount);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObjects")));

        simpleObjectDAO.save(resultSimpleObject);

        Assert.assertEquals(1, simpleObjectDAO.findById(simpleObject.getId()).getRelatedObjects().size());
    }


    @Test
    public void testSetInIterator()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(999);

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(123);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.addRelatedObject(relatedObject);
        simpleObject.addRelatedObject(relatedObject2);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        int iteratedItemCount = 0;
        for (ListIterator<RelatedObject> roIterator = resultSimpleObject.getRelatedObjects().listIterator(); roIterator.hasNext(); ) {
            iteratedItemCount++;
            RelatedObject ro = roIterator.next();

            if (ro.getRelatedIntValue() < 500) {
                RelatedObject relatedObject3 = new RelatedObject();
                relatedObject3.setRelatedIntValue(ro.getRelatedIntValue() + 1);

                roIterator.set(relatedObject3);
            }
        }

        Assert.assertEquals(2, iteratedItemCount);

        //noinspection unchecked
        assertKeys(((DirtyableDBObject) resultSimpleObject).getDirtyKeys(), new HashSet(Collections.singleton("relatedObjects")));

        simpleObjectDAO.save(resultSimpleObject);

        List<RelatedObject> relatedObjectList = simpleObjectDAO.findById(simpleObject.getId()).getRelatedObjects();

        Assert.assertEquals(2, relatedObjectList.size());
        Assert.assertEquals(999, relatedObjectList.get(0).getRelatedIntValue());
        Assert.assertEquals(124, relatedObjectList.get(1).getRelatedIntValue());
    }


    @Test
    public void testClearAndReAddSameKey() {
        final RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(1);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setRelatedObjectMap(new HashMap<String, RelatedObject>() {{
            put("first", relatedObject1);
        }});

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        resultSimpleObject.getRelatedObjectMap().clear();

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(2);

        resultSimpleObject.getRelatedObjectMap().put("first", relatedObject2);

        simpleObjectDAO.save(resultSimpleObject);

        resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertEquals(1, resultSimpleObject.getRelatedObjectMap().size());
        Assert.assertTrue(resultSimpleObject.getRelatedObjectMap().containsKey("first"));
        Assert.assertEquals(2, resultSimpleObject.getRelatedObjectMap().get("first").getRelatedIntValue());
    }


    @Test
    public void testAddThenRemoveSameKey() {
        final RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(1);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setRelatedObjectMap(new HashMap<String, RelatedObject>() {{
            put("first", relatedObject1);
        }});

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(2);

        resultSimpleObject.getRelatedObjectMap().put("second", relatedObject2);
        resultSimpleObject.getRelatedObjectMap().remove("second");

        simpleObjectDAO.save(resultSimpleObject);

        resultSimpleObject = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertEquals(1, resultSimpleObject.getRelatedObjectMap().size());
        Assert.assertTrue(resultSimpleObject.getRelatedObjectMap().containsKey("first"));
        Assert.assertEquals(1, resultSimpleObject.getRelatedObjectMap().get("first").getRelatedIntValue());
    }


    @Test
    public void testEquals() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        Assert.assertTrue(simpleObject.equals(simpleObject));

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject1 = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertTrue(simpleObject.equals(resultSimpleObject1));
        Assert.assertTrue(resultSimpleObject1.equals(simpleObject));

        SimpleObject resultSimpleObject2 = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertTrue(resultSimpleObject1.equals(resultSimpleObject2));
        Assert.assertTrue(resultSimpleObject2.equals(resultSimpleObject1));
    }


    @Test
    public void testCollectionsEquals() {
        RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(1);
        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(2);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.addRelatedObject(relatedObject1);
        simpleObject.addRelatedObject(relatedObject2);
        simpleObject.addToRelatedObjectSet(relatedObject1);
        simpleObject.addToRelatedObjectSet(relatedObject2);
        simpleObject.addRelatedObject("one", relatedObject1);
        simpleObject.addRelatedObject("two", relatedObject2);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultSimpleObject1 = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertTrue(simpleObject.getRelatedObjects().equals(resultSimpleObject1.getRelatedObjects()));
        Assert.assertTrue(resultSimpleObject1.getRelatedObjects().equals(simpleObject.getRelatedObjects()));
        Assert.assertTrue(simpleObject.getRelatedObjectSet().equals(resultSimpleObject1.getRelatedObjectSet()));
        Assert.assertTrue(resultSimpleObject1.getRelatedObjectSet().equals(simpleObject.getRelatedObjectSet()));
        Assert.assertTrue(simpleObject.getRelatedObjectMap().equals(resultSimpleObject1.getRelatedObjectMap()));
        Assert.assertTrue(resultSimpleObject1.getRelatedObjectMap().equals(simpleObject.getRelatedObjectMap()));

        SimpleObject resultSimpleObject2 = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertTrue(resultSimpleObject2.getRelatedObjects().equals(resultSimpleObject1.getRelatedObjects()));
        Assert.assertTrue(resultSimpleObject1.getRelatedObjects().equals(resultSimpleObject2.getRelatedObjects()));
        Assert.assertTrue(resultSimpleObject2.getRelatedObjectSet().equals(resultSimpleObject1.getRelatedObjectSet()));
        Assert.assertTrue(resultSimpleObject1.getRelatedObjectSet().equals(resultSimpleObject2.getRelatedObjectSet()));
        Assert.assertTrue(resultSimpleObject2.getRelatedObjectMap().equals(resultSimpleObject1.getRelatedObjectMap()));
        Assert.assertTrue(resultSimpleObject1.getRelatedObjectMap().equals(resultSimpleObject2.getRelatedObjectMap()));
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
