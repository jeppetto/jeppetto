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

package org.iternine.jeppetto.dao.test;


import org.iternine.jeppetto.dao.annotation.Transient;

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
    private double doubleValue;
    private RelatedObject relatedObject;
    private List<RelatedObject> relatedObjects;
    private Map<String, RelatedObject> relatedObjectMap;
    private Set<RelatedObject> relatedObjectSet;
    private List<String> stringList;
    private Map<String, String> stringMap;
    private Set<String> stringSet;
    private SimpleEnum simpleEnum;
    private int transientValue;


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


    public double getDoubleValue() {
        return doubleValue;
    }


    public void setDoubleValue(double doubleValue) {
        this.doubleValue = doubleValue;
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


    @Transient
    public int getTransientValue() {
        return transientValue;
    }


    public void setTransientValue(int transientValue) {
        this.transientValue = transientValue;
    }


    public void addToStringList(String string) {
        if (stringList == null) {
            stringList = new ArrayList<String>();
        }

        stringList.add(string);
    }


    public List<String> getStringList() {
        return stringList;
    }


    public void setStringList(List<String> stringList) {
        this.stringList = stringList;
    }


    public Map<String, String> getStringMap() {
        return stringMap;
    }


    public void setStringMap(Map<String, String> stringMap) {
        this.stringMap = stringMap;
    }


    public void addToStringSet(String string) {
        if (stringSet == null) {
            stringSet = new HashSet<String>();
        }

        stringSet.add(string);
    }


    public Set<String> getStringSet() {
        return stringSet;
    }


    public void setStringSet(Set<String> stringSet) {
        this.stringSet = stringSet;
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
        sb.append(", anotherIntValue=").append(anotherIntValue);
        sb.append(", longValue=").append(longValue);
        sb.append(", relatedObject=").append(relatedObject);
        sb.append(", relatedObjects=").append(relatedObjects);
        sb.append(", relatedObjectMap=").append(relatedObjectMap);
        sb.append(", stringList=").append(stringList);
        sb.append(", stringMap=").append(stringMap);
        sb.append(", stringSet=").append(stringSet);
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
