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

package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObject;
import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleEnum;
import org.iternine.jeppetto.dao.test.SimpleObject;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class MongoDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
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
                                      new MongoDatabaseProvider());

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
    public void saveAndFindById()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        simpleObject.setIntValue(1234);

        simpleObjectDAO.save(simpleObject);

        assertNotNull(simpleObject.getId());

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
        assertDirty(resultObject, false);

        resultObject.setIntValue(4321);

        assertDirty(resultObject, true);
    }


    @Test
    public void fieldChangesAreDirty()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject;

        resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertDirty(resultObject, false);
        assertEquals(1234, resultObject.getIntValue());

        resultObject.setSimpleEnum(SimpleEnum.EnumValue);

        assertDirty(resultObject, true);

        simpleObjectDAO.save(resultObject);

        assertDirty(resultObject, false);

        resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertDirty(resultObject, false);
        assertEquals(1234, resultObject.getIntValue());
        assertEquals(SimpleEnum.EnumValue, resultObject.getSimpleEnum());

        resultObject.setIntValue(9876);

        assertDirty(resultObject, true);

        simpleObjectDAO.save(resultObject);

        assertDirty(resultObject, false);

        resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertDirty(resultObject, false);
        assertEquals(9876, resultObject.getIntValue());
    }


    @Test
    public void mapChangesAreDirty()
            throws NoSuchItemException {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(5678);

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);
        simpleObject.setStringMap(new HashMap<String, String>());
        simpleObject.getStringMap().put("foo", "bar");
        simpleObject.setRelatedObjectMap(new HashMap<String, RelatedObject>());
        simpleObject.getRelatedObjectMap().put("foo", relatedObject);

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject;

        resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertEquals("bar", resultObject.getStringMap().get("foo"));
        assertDirty(resultObject, false);

        resultObject.getStringMap().put("baz", "bat");

        assertDirty(resultObject, true);

        simpleObjectDAO.save(resultObject);

        resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertEquals("bar", resultObject.getStringMap().get("foo"));
        assertEquals("bat", resultObject.getStringMap().get("baz"));
        assertDirty(resultObject, false);

        resultObject.getStringMap().putAll(new HashMap<String, String>());

        assertDirty(resultObject, false);

        resultObject.getRelatedObjectMap().put("baz", relatedObject);

        assertDirty(resultObject, true);

        resultObject.addToRelatedObjectSet(new RelatedObject());
        resultObject.addToRelatedObjectSet(new RelatedObject());

        simpleObjectDAO.save(resultObject);

        resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertEquals(5678, resultObject.getRelatedObjectMap().get("baz").getRelatedIntValue());
    }


    @Test
    public void objectIsWellBehavedDBObject()
            throws NoSuchItemException {
        SimpleObject pojo = new SimpleObject();
        pojo.setIntValue(1234);
        pojo.setSimpleEnum(SimpleEnum.EnumValue);

        simpleObjectDAO.save(pojo);

        pojo = simpleObjectDAO.findById(pojo.getId());

        DBObject dbo = (DBObject) pojo;

        assertEquals(pojo.getId(), dbo.get("_id").toString());
        assertEquals("EnumValue", dbo.get("simpleEnum"));

        Set<String> knownFields = new HashSet<String>();
        Collections.addAll(knownFields, "_id",
                                        "testBoolean",
                                        "__olv",
                                        "stringMap",
                                        "relatedObjects",
                                        "relatedObject",
                                        "relatedObjectMap",
                                        "relatedObjectSet",
                                        "stringList",
                                        "stringMap",
                                        "stringSet",
                                        "simpleEnum",
                                        "bytes",
                                        "intValue",
                                        "longValue",
                                        "doubleValue",
                                        "stringValue",
                                        "anotherIntValue");

        assertEquals(knownFields, dbo.keySet());

        ObjectId newId = ObjectId.get();
        dbo.put("_id", newId);

        assertEquals(newId, dbo.get("_id"));
        assertEquals("EnumValue", dbo.removeField("simpleEnum"));
        assertFalse(dbo.keySet().contains("simpleEnum"));
    }


    private void assertDirty(Object obj, boolean dirty) {
        assertTrue(obj instanceof DirtyableDBObject);
        assertEquals(dirty, ((DirtyableDBObject) obj).isDirty());
    }
}
