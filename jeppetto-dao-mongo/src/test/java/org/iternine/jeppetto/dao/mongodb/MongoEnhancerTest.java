/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
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


import org.iternine.jeppetto.dao.mongodb.enhance.DBObjectUtil;
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObject;
import org.iternine.jeppetto.dao.mongodb.enhance.EnhancerHelper;
import org.iternine.jeppetto.enhance.Enhancer;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class MongoEnhancerTest {

    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    @Test
    public void idGetsPassedThrough() {
        Pojo pojo = new Pojo();
        Pojo enhanced = EnhancerHelper.getDirtyableDBObjectEnhancer(Pojo.class).enhance(pojo);
        DBObject dbo = (DBObject) enhanced;
        ObjectId id = ObjectId.get();
        dbo.put("_id", id);
        assertEquals(id, dbo.get("_id"));
        assertEquals(id.toString(), enhanced.getId());
        assertEquals(id.toString(), pojo.getId());
        id = ObjectId.get();
        pojo.setId(id.toString());
        assertEquals(id.toString(), enhanced.getId());
        assertEquals(id.toString(), pojo.getId());
    }


    @Test
    public void mapKeyedByEnum() {
        Pojo pojo = new Pojo();
        Map<PojoEnum, Integer> map = new HashMap<PojoEnum, Integer>();
        map.put(PojoEnum.Foo, 1);
        map.put(PojoEnum.Bar, 2);
        pojo.setMap(map);
        Pojo enhanced = EnhancerHelper.getDirtyableDBObjectEnhancer(Pojo.class).enhance(pojo);
        BasicDBObject dbo = new BasicDBObject(((DBObject) enhanced).toMap());
        Pojo roundTrip = (Pojo) DBObjectUtil.fromObject(Pojo.class, dbo);
        assertEquals(1, roundTrip.getMap().get(PojoEnum.Foo).intValue());
        assertEquals(2, roundTrip.getMap().get(PojoEnum.Bar).intValue());
    }


    @Test
    public void enhanceClassWithMapOfStringToString() {
        WithMapOfStringToString foo = new WithMapOfStringToString();
        foo.setMap(new HashMap<String, String>());
        foo.getMap().put("foo", "bar");
        WithMapOfStringToString foo2 = EnhancerHelper.getDirtyableDBObjectEnhancer(WithMapOfStringToString.class).enhance(foo);
        Map<String, String> map = foo2.getMap();
        assertEquals("bar", map.get("foo"));
    }


    @Test
    public void mapOfStringToStringIsMutable() {
        WithMapOfStringToString foo = new WithMapOfStringToString();
        foo.setMap(new HashMap<String, String>());
        foo.getMap().put("foo", "bar");
        WithMapOfStringToString foo2 = EnhancerHelper.getDirtyableDBObjectEnhancer(WithMapOfStringToString.class).enhance(foo);
        Map<String, String> map = foo2.getMap();
        map.put("biz", "baz");
        assertEquals(2, foo2.getMap().size());
    }


    @Test
    public void classWithSelfReferentialGetter() throws Throwable {
        Enhancer<Circular> enhancer = EnhancerHelper.getDirtyableDBObjectEnhancer(Circular.class);
        Circular c1 = enhancer.newInstance();
        c1.setParent(enhancer.newInstance());
        Circular c2 = c1.getParent();
        ((DirtyableDBObject) c2).markPersisted();
        assertDirty(c1);
        assertNotDirty(c2);
    }


    @Test
    public void objectWithBigDecimal() {
        Enhancer<BigD> enhancer = EnhancerHelper.getDirtyableDBObjectEnhancer(BigD.class);
        BigD bd1 = new BigD();
        bd1.setN(BigDecimal.valueOf(3.14159265d));
        BigD bd1e = enhancer.enhance(bd1);
        DBObject dbo = (DBObject) bd1e;
        assertEquals(dbo.get("n"), bd1.getN());
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void assertDirty(Circular c1) {
        assertTrue(c1 instanceof DirtyableDBObject);
        assertTrue(((DirtyableDBObject) c1).isDirty());
    }


    private void assertNotDirty(Circular c1) {
        assertTrue(c1 instanceof DirtyableDBObject);
        assertFalse(((DirtyableDBObject) c1).isDirty());
    }


    //-------------------------------------------------------------
    // Inner Class - PojoEnum
    //-------------------------------------------------------------

    public static enum PojoEnum {
        Foo,
        Bar
    }


    //-------------------------------------------------------------
    // Inner Class - Pojo
    //-------------------------------------------------------------

    public static class Pojo {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private String id;
        private String todo;
        private Map<PojoEnum, Integer> map;


        //-------------------------------------------------------------
        // Methods - Getter/Setter
        //-------------------------------------------------------------

        public String getId() {
            return id;
        }


        public void setId(String id) {
            this.id = id;
        }


        public String getTodo() {
            return todo;
        }


        public void setTodo(String todo) {
            this.todo = todo;
        }


        public Map<PojoEnum, Integer> getMap() {
            return map;
        }


        public void setMap(Map<PojoEnum, Integer> map) {
            this.map = map;
        }
    }


    //-------------------------------------------------------------
    // Inner Class - BigD
    //-------------------------------------------------------------

    public static class BigD {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private BigDecimal n;


        //-------------------------------------------------------------
        // Methods - Getter/Setter
        //-------------------------------------------------------------

        public BigDecimal getN() {
            return n;
        }


        public void setN(BigDecimal n) {
            this.n = n;
        }
    }


    //-------------------------------------------------------------
    // Inner Class - WithMapOfStringToString
    //-------------------------------------------------------------

    public static class WithMapOfStringToString {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private Map<String, String> map;


        //-------------------------------------------------------------
        // Methods - Getter/Setter
        //-------------------------------------------------------------

        public Map<String, String> getMap() {
            return map;
        }


        public void setMap(Map<String, String> map) {
            this.map = map;
        }
    }


    //-------------------------------------------------------------
    // Inner Class - Circular
    //-------------------------------------------------------------

    public static class Circular {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private Circular parent;


        //-------------------------------------------------------------
        // Methods - Getter/Setter
        //-------------------------------------------------------------

        public Circular getParent() {
            return parent;
        }


        public void setParent(Circular parent) {
            this.parent = parent;
        }
    }
}
