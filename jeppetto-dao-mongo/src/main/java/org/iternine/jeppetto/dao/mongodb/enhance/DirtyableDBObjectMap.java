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

package org.iternine.jeppetto.dao.mongodb.enhance;


import org.bson.BSONObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


@SuppressWarnings({ "unchecked" })
public class DirtyableDBObjectMap
        implements Map, DirtyableDBObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private boolean dirty;
    private Map delegate;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DirtyableDBObjectMap() {
        this.delegate = new HashMap();
    }


    public DirtyableDBObjectMap(Map delegate) {
        this.delegate = delegate;
    }


    //-------------------------------------------------------------
    // Implementation - Map
    //-------------------------------------------------------------

    @Override
    public Object put(Object key, Object value) {
        dirty = true;

        return delegate.put(key, value);
    }


    @Override
    public Object remove(Object key) {
        Object result = delegate.remove(key);

        if (result != null) {
            dirty = true;
        }

        return result;
    }


    @Override
    public void putAll(Map m) {
        if (!m.isEmpty()) {
            dirty = true;
        }

        delegate.putAll(m);
    }


    @Override
    public void clear() {
        if (!isEmpty()) {
            dirty = true;
        }

        delegate.clear();
    }


    @Override
    public int size() {
        return delegate.size();
    }


    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }


    @Override
    public boolean containsKey(Object o) {
        return delegate.containsKey(o);
    }


    @Override
    public boolean containsValue(Object o) {
        return delegate.containsValue(o);
    }


    @Override
    public Object get(Object o) {
        return delegate.get(o);
    }


    @Override
    public Set keySet() {
        // TODO: verify set isn't modified
        return delegate.keySet();
    }


    @Override
    public Collection values() {
        // TODO: verify collection isn't modified
        return delegate.values();
    }


    @Override
    public Set entrySet() {
        // TODO: verify set isn't modified
        return delegate.entrySet();
    }


    //-------------------------------------------------------------
    // Implementation - DirtyableDBObject
    //-------------------------------------------------------------

    @Override
    public boolean isDirty() {
        return dirty;
    }


    @Override
    public void markCurrentAsClean() {
        dirty = false;
    }


    //-------------------------------------------------------------
    // Implementation - DBObject
    //-------------------------------------------------------------

    @Override
    public void markAsPartialObject() {
        throw new RuntimeException("Can't mark DirtyableMap as partial");
    }


    @Override
    public boolean isPartialObject() {
        return false;
    }


    @Override
    public Object put(String key, Object value) {
        dirty = true;

        return delegate.put(key, value);
    }


    @Override
    public void putAll(BSONObject bsonObject) {
        Set<String> keys = bsonObject.keySet();

        if (keys.isEmpty()) {
            return;
        }

        dirty = true;

        for (String key : keys) {
            delegate.put(key, bsonObject.get(key));
        }
    }


    @Override
    public Object get(String key) {
        return delegate.get(key);
    }


    @Override
    public Map toMap() {
        return this;
    }


    @Override
    public Object removeField(String key) {
        return remove(key);
    }


    @Override
    public boolean containsKey(String key) {
        return delegate.containsKey(key);
    }


    @Override
    public boolean containsField(String field) {
        return delegate.containsKey(field);
    }
}
