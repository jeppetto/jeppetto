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

package org.iternine.jeppetto.dao.test.updateobject;


import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleEnum;
import org.iternine.jeppetto.dao.test.SimpleObject;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public abstract class UpdateObjectDAOTest {

    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract UpdateObjectDAO getSimpleObjectReferencesDAO();

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
    public void updateSimpleFields() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);
        simpleObject.setLongValue(1l);
        simpleObject.setSimpleEnum(SimpleEnum.EnumValue);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();
        updateObject.setLongValue(Long.MAX_VALUE);
        updateObject.setAnotherIntValue(999);
        updateObject.setSimpleEnum(null);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertEquals(123, resultSimpleObject.getIntValue());
        assertEquals(Long.MAX_VALUE, resultSimpleObject.getLongValue());
        assertEquals(999, resultSimpleObject.getAnotherIntValue());
        assertEquals(null, resultSimpleObject.getSimpleEnum());
    }


    @Test
    public void updateSimpleFieldsInMultipleEntities() {
        SimpleObject simpleObject1 = new SimpleObject();
        simpleObject1.setIntValue(123);
        simpleObject1.setLongValue(1l);
        getSimpleObjectReferencesDAO().save(simpleObject1);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject2.setIntValue(234);
        simpleObject2.setLongValue(2l);
        getSimpleObjectReferencesDAO().save(simpleObject2);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();
        updateObject.setLongValue(Long.MAX_VALUE);
        updateObject.setAnotherIntValue(999);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject1.getId(), simpleObject2.getId());

        SimpleObject resultSimpleObject1 = getSimpleObjectReferencesDAO().findById(simpleObject1.getId());

        assertEquals(123, resultSimpleObject1.getIntValue());
        assertEquals(Long.MAX_VALUE, resultSimpleObject1.getLongValue());
        assertEquals(999, resultSimpleObject1.getAnotherIntValue());

        SimpleObject resultSimpleObject2 = getSimpleObjectReferencesDAO().findById(simpleObject2.getId());

        assertEquals(234, resultSimpleObject2.getIntValue());
        assertEquals(Long.MAX_VALUE, resultSimpleObject2.getLongValue());
        assertEquals(999, resultSimpleObject2.getAnotherIntValue());
    }


    @Test
    public void addNestedObject() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        updateObject.setRelatedObject(relatedObject);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertEquals(123, resultSimpleObject.getIntValue());
        assertNotNull(resultSimpleObject.getRelatedObject());
        assertEquals(456, resultSimpleObject.getRelatedObject().getRelatedIntValue());
    }


    @Test
    public void updateNestedObject() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        simpleObject.setRelatedObject(relatedObject);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        updateObject.setIntValue(456);
        RelatedObject relatedUpdateObject = updateObject.getRelatedObject();
        relatedUpdateObject.setRelatedIntValue(999);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertEquals(456, resultSimpleObject.getIntValue());
        assertEquals(999, resultSimpleObject.getRelatedObject().getRelatedIntValue());
    }


    @Test
    public void addNewList() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        List<RelatedObject> relatedObjects = new ArrayList<RelatedObject>();
        relatedObjects.add(relatedObject);

        updateObject.setRelatedObjects(relatedObjects);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjects());
        assertEquals(1, resultSimpleObject.getRelatedObjects().size());
        assertEquals(456, resultSimpleObject.getRelatedObjects().get(0).getRelatedIntValue());
    }


    @Test
    public void addToExistingList() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        List<RelatedObject> relatedObjects = new ArrayList<RelatedObject>();
        relatedObjects.add(relatedObject);

        simpleObject.setRelatedObjects(relatedObjects);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        List<RelatedObject> relatedObjectUpdate = updateObject.getRelatedObjects();

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(789);

        relatedObjectUpdate.add(relatedObject2);

        RelatedObject relatedObject3 = new RelatedObject();
        relatedObject3.setRelatedIntValue(999);

        relatedObjectUpdate.add(relatedObject3);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjects());
        assertEquals(3, resultSimpleObject.getRelatedObjects().size());
        assertEquals(456, resultSimpleObject.getRelatedObjects().get(0).getRelatedIntValue());
        assertEquals(789, resultSimpleObject.getRelatedObjects().get(1).getRelatedIntValue());
        assertEquals(999, resultSimpleObject.getRelatedObjects().get(2).getRelatedIntValue());
    }


    @Test
    public void setAtIndex() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        List<RelatedObject> relatedObjects = new ArrayList<RelatedObject>();
        relatedObjects.add(relatedObject);

        simpleObject.setRelatedObjects(relatedObjects);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(789);

        updateObject.getRelatedObjects().set(3, relatedObject2);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjects());
        assertEquals(456, resultSimpleObject.getRelatedObjects().get(0).getRelatedIntValue());

        // TODO: Different stores treat gaps differently.  Some compact them down, others leave nulls in the middle
        if (resultSimpleObject.getRelatedObjects().size() == 2) {
            assertEquals(789, resultSimpleObject.getRelatedObjects().get(1).getRelatedIntValue());
        } else {
            assertEquals(4, resultSimpleObject.getRelatedObjects().size());
            assertNull(resultSimpleObject.getRelatedObjects().get(1));
            assertNull(resultSimpleObject.getRelatedObjects().get(2));
            assertEquals(789, resultSimpleObject.getRelatedObjects().get(3).getRelatedIntValue());
        }
    }


    @Test
    public void removeFromExistingList() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(456);
        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(789);

        List<RelatedObject> relatedObjects = new ArrayList<RelatedObject>();
        relatedObjects.add(relatedObject1);
        relatedObjects.add(relatedObject2);

        simpleObject.setRelatedObjects(relatedObjects);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        List<RelatedObject> relatedObjectUpdate = updateObject.getRelatedObjects();

        relatedObjectUpdate.remove(relatedObject1);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjects());
        assertEquals(1, resultSimpleObject.getRelatedObjects().size());
        assertEquals(789, resultSimpleObject.getRelatedObjects().get(0).getRelatedIntValue());
    }


    @Test
    public void removeFromExistingListUsingIndex() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(456);
        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(789);

        List<RelatedObject> relatedObjects = new ArrayList<RelatedObject>();
        relatedObjects.add(relatedObject1);
        relatedObjects.add(relatedObject2);

        simpleObject.setRelatedObjects(relatedObjects);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        List<RelatedObject> relatedObjectUpdate = updateObject.getRelatedObjects();

        relatedObjectUpdate.remove(0);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjects());
        assertEquals(1, resultSimpleObject.getRelatedObjects().size());
        assertEquals(789, resultSimpleObject.getRelatedObjects().get(0).getRelatedIntValue());
    }


    @Test
    public void clearList() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        List<RelatedObject> relatedObjects = new ArrayList<RelatedObject>();
        relatedObjects.add(relatedObject);

        simpleObject.setRelatedObjects(relatedObjects);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        updateObject.getRelatedObjects().clear();
        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjects());
        assertEquals(0, resultSimpleObject.getRelatedObjects().size());
    }


    @Test
    public void clearAndAddToList() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        List<RelatedObject> relatedObjects = new ArrayList<RelatedObject>();
        relatedObjects.add(relatedObject);

        simpleObject.setRelatedObjects(relatedObjects);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        updateObject.getRelatedObjects().clear();

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(789);

        updateObject.getRelatedObjects().add(relatedObject2);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjects());
        assertEquals(1, resultSimpleObject.getRelatedObjects().size());
        assertEquals(789, resultSimpleObject.getRelatedObjects().get(0).getRelatedIntValue());
    }


    @Test
    public void clearMap() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        Map<String, RelatedObject> relatedObjectMap = new HashMap<String, RelatedObject>();
        relatedObjectMap.put("one", relatedObject);

        simpleObject.setRelatedObjectMap(relatedObjectMap);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        updateObject.getRelatedObjectMap().clear();
        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjectMap());
        assertEquals(0, resultSimpleObject.getRelatedObjectMap().size());
    }


    @Test
    public void clearAndAddToMap() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(456);

        Map<String, RelatedObject> relatedObjectMap = new HashMap<String, RelatedObject>();
        relatedObjectMap.put("one", relatedObject);

        simpleObject.setRelatedObjectMap(relatedObjectMap);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        updateObject.getRelatedObjectMap().clear();

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(789);

        updateObject.getRelatedObjectMap().put("two", relatedObject2);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getRelatedObjectMap());
        assertEquals(1, resultSimpleObject.getRelatedObjectMap().size());
        assertEquals(789, resultSimpleObject.getRelatedObjectMap().get("two").getRelatedIntValue());
    }


    @Test
    public void addStringsToList() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);
        simpleObject.addToStringList("duck");
        simpleObject.addToStringList("bunny");

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        List<String> updateList = updateObject.getStringList();
        updateList.add("bunny");
        updateList.add("cow");

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getStringList());
        assertEquals(4, resultSimpleObject.getStringList().size());  // duck, bunny, bunny, cow
        assertEquals("duck", resultSimpleObject.getStringList().get(0));
        assertEquals("bunny", resultSimpleObject.getStringList().get(1));
        assertEquals("bunny", resultSimpleObject.getStringList().get(2));
        assertEquals("cow", resultSimpleObject.getStringList().get(3));
    }



    @Test
    public void addStringsToSet() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(123);
        simpleObject.addToStringSet("duck");
        simpleObject.addToStringSet("bunny");

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();

        Set<String> updateSet = updateObject.getStringSet();
        updateSet.add("bunny"); // should not be duplicated
        updateSet.add("cow");

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertNotNull(resultSimpleObject.getStringSet());
        assertEquals(3, resultSimpleObject.getStringSet().size());  // duck, bunny, cow
        assertTrue(resultSimpleObject.getStringSet().contains("duck"));
        assertTrue(resultSimpleObject.getStringSet().contains("bunny"));
        assertTrue(resultSimpleObject.getStringSet().contains("cow"));
    }


    @Test
    public void updateByOtherField() {
        SimpleObject simpleObject1 = new SimpleObject();
        simpleObject1.setIntValue(123);
        simpleObject1.setAnotherIntValue(-1);

        getSimpleObjectReferencesDAO().save(simpleObject1);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject2.setIntValue(234);
        simpleObject2.setAnotherIntValue(-1);

        getSimpleObjectReferencesDAO().save(simpleObject2);

        SimpleObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();
        updateObject.setAnotherIntValue(42);

        getSimpleObjectReferencesDAO().updateByAnotherIntValue(updateObject, -1);

        Iterable<SimpleObject> results = getSimpleObjectReferencesDAO().findAll();

        int resultCount = 0;
        for (SimpleObject result : results) {
            assertEquals(42, result.getAnotherIntValue());
            assertTrue(result.getIntValue() == 123 || result.getIntValue() == 234);

            resultCount++;
        }

        assertEquals(2, resultCount);
    }


    @Test
    public void incrementIntValue() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleUpdateObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();
        updateObject.addToIntValue(1);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertEquals(2, resultSimpleObject.getIntValue());
    }


    @Test
    public void decrementIntValue() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(100);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleUpdateObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();
        updateObject.addToIntValue(-5);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertEquals(95, resultSimpleObject.getIntValue());
    }


    @Test
    public void incrementLongValue() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setLongValue(1l);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleUpdateObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();
        updateObject.addToLongValue(100l);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertEquals(101l, resultSimpleObject.getLongValue());
    }


    @Test
    public void incrementDoubleValue() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setDoubleValue(1.5d);

        getSimpleObjectReferencesDAO().save(simpleObject);

        SimpleUpdateObject updateObject = getSimpleObjectReferencesDAO().getUpdateObject();
        updateObject.addToDoubleValue(2.25d);

        getSimpleObjectReferencesDAO().updateByIds(updateObject, simpleObject.getId());

        SimpleObject resultSimpleObject = getSimpleObjectReferencesDAO().findById(simpleObject.getId());

        assertEquals(3.75d, resultSimpleObject.getDoubleValue(), 0.0001d);
    }


    // TODO: index based operation on existing list
    // TODO: replace list

    // TODO: test merging of items (e.g. two different $pushAlls) for set, list, map
}
