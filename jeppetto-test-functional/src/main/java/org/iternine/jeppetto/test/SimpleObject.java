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

package org.iternine.jeppetto.test;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class SimpleObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String id;
    private int intValue;
    private int anotherIntValue;
    private long longValue;
    private RelatedObject relatedObject;
    private List<RelatedObject> relatedObjects;
    private Map<String, RelatedObject> relatedObjectMap;
    private Map<String, String> stringMap;
    private Set<RelatedObject> relatedObjectSet;
    private SimpleEnum simpleEnum;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public SimpleObject() {
    }


    public SimpleObject(int intValue) {
        this.intValue = intValue;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public boolean isTestBoolean() {
        return true;
    }


    public void setTestBoolean(boolean testBoolean) {
        // ignore, this is just in here for testing
    }


    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }


    public int getIntValue() {
        return intValue;
    }


    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }


    public int getAnotherIntValue() {
        return anotherIntValue;
    }


    public long getLongValue() {
        return longValue;
    }


    public void setLongValue(long longValue) {
        this.longValue = longValue;
    }


    public void setAnotherIntValue(int anotherIntValue) {
        this.anotherIntValue = anotherIntValue;
    }


    public RelatedObject getRelatedObject() {
        return relatedObject;
    }


    public void setRelatedObject(RelatedObject relatedObject) {
        this.relatedObject = relatedObject;
    }


    public void addRelatedObject(RelatedObject relatedObject) {
        if (relatedObjects == null) {
            relatedObjects = new ArrayList<RelatedObject>();
        }

        relatedObjects.add(relatedObject);
    }


    public List<RelatedObject> getRelatedObjects() {
        return relatedObjects;
    }


    public void setRelatedObjects(List<RelatedObject> relatedObjects) {
        this.relatedObjects = relatedObjects;
    }


    public void addRelatedObject(String key, RelatedObject relatedObject) {
        if (relatedObjectMap == null) {
            relatedObjectMap = new HashMap<String, RelatedObject>();
        }

        relatedObjectMap.put(key, relatedObject);
    }


    public Map<String, RelatedObject> getRelatedObjectMap() {
        return relatedObjectMap;
    }


    public void setRelatedObjectMap(Map<String, RelatedObject> relatedObjectMap) {
        this.relatedObjectMap = relatedObjectMap;
    }


    public SimpleEnum getSimpleEnum() {
        return simpleEnum;
    }


    public void setSimpleEnum(SimpleEnum simpleEnum) {
        this.simpleEnum = simpleEnum;
    }


    public Map<String, String> getStringMap() {
        return stringMap;
    }


    public void setStringMap(Map<String, String> stringMap) {
        this.stringMap = stringMap;
    }


    public Set<RelatedObject> getRelatedObjectSet() {
        return relatedObjectSet;
    }


    public void setRelatedObjectSet(Set<RelatedObject> relatedObjectSet) {
        this.relatedObjectSet = relatedObjectSet;
    }


    public void addToRelatedObjectSet(RelatedObject relatedObject) {
        if (relatedObjectSet == null) {
            relatedObjectSet = new HashSet<RelatedObject>();
        }

        relatedObjectSet.add(relatedObject);
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("SimpleObject");
        sb.append("{id='").append(id).append('\'');
        sb.append(", intValue=").append(intValue);
        sb.append(", relatedObject=").append(relatedObject);
        sb.append(", relatedObjects=").append(relatedObjects);
        sb.append(", relatedObjectMap=").append(relatedObjectMap);
        sb.append(", stringMap=").append(stringMap);
        sb.append(", simpleEnum=").append(simpleEnum);
        sb.append('}');
        return sb.toString();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof SimpleObject)) {
            return false;
        }

        SimpleObject that = (SimpleObject) o;

        return id != null && id.equals(that.id);
    }


    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
