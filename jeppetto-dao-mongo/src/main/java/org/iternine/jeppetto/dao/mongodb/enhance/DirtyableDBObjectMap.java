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


import org.iternine.jeppetto.dao.JeppettoException;

import org.bson.BSONObject;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * Despite the lack of explicit generic types, this acts like a Map<String, Object> due to the way
 * MongoDB expects the keys to be expressed.  ClassCastExceptions may occur if this is violated.
 */
@SuppressWarnings({ "unchecked" })
public class DirtyableDBObjectMap
        implements Map, DirtyableDBObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Map<String, Object> delegate;
    private Set<String> addedOrUpdatedKeys = new HashSet<String>();
    private Set<String> removedKeys = new HashSet<String>();
    private boolean persisted;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DirtyableDBObjectMap() {
        this(new HashMap<String, Object>());
    }


    public DirtyableDBObjectMap(Map<String, Object> delegate) {
        this.delegate = delegate;
    }


    //-------------------------------------------------------------
    // Implementation - Map
    //-------------------------------------------------------------

    @Override
    public Object put(Object key, Object value) {
        String stringKey = (String) key;

        addedOrUpdatedKeys.add(stringKey);

        return delegate.put(stringKey, value);
    }


    @Override
    public Object remove(Object key) {
        String stringKey = (String) key;
        Object result = delegate.remove(stringKey);

        if (result != null) {
            removedKeys.add(stringKey);
        }

        return result;
    }


    @Override
    public void putAll(Map m) {
        for (Object o : m.entrySet()) {
            Entry entry = (Entry) o;

            put(entry.getKey(), entry.getValue());
        }
    }


    @Override
    public void clear() {
        removedKeys.addAll(delegate.keySet());
        addedOrUpdatedKeys.clear();
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
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }


    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }


    @Override
    public Object get(Object key) {
        Object value = delegate.get(key);

        if (value == null || value instanceof DirtyableDBObject || DBObjectUtil.needsNoConversion(value.getClass())) {
            return value;
        }

        // TODO: revisit whether these semantics makes sense
        Object converted = DBObjectUtil.toDBObject(value);
        delegate.put((String) key, converted);

        return converted;
    }


    @Override
    public Set keySet() {
        // TODO: handle case when set is modified
        return delegate.keySet();
    }


    @Override
    public Collection values() {
        // TODO: convert items to a DirtyableDBObject...
        // TODO: handle case when set is modified
        return delegate.values();
    }


    @Override
    public Set entrySet() {
        return new AbstractSet() {
            @Override
            public Iterator iterator() {
                return new Iterator() {
                    private Iterator<Entry<String, Object>> delegateIterator = delegate.entrySet().iterator();
                    private String currentKey;

                    @Override
                    public boolean hasNext() {
                        return delegateIterator.hasNext();
                    }


                    @Override
                    public Object next() {
                        Entry<String, Object> current = delegateIterator.next();
                        this.currentKey = current.getKey();
                        final Object currentValue;

                        if (current.getValue() instanceof DirtyableDBObject || DBObjectUtil.needsNoConversion(current.getValue().getClass())) {
                            currentValue = current.getValue();
                        } else {
                            currentValue = DBObjectUtil.toDBObject(current.getValue());

                            delegate.put(currentKey, currentValue);
                        }

                        return new Entry<String, Object>() {
                            @Override
                            public String getKey() {
                                return currentKey;
                            }


                            @Override
                            public Object getValue() {
                                return currentValue;
                            }


                            @Override
                            public Object setValue(Object value) {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }


                    @Override
                    public void remove() {
                        DirtyableDBObjectMap.this.remove(currentKey);
                    }
                };
            }


            @Override
            public int size() {
                return delegate.size();
            }


            @Override
            public boolean remove(Object o) {
                return DirtyableDBObjectMap.this.remove(o) != null;
            }


            @Override
            public void clear() {
                DirtyableDBObjectMap.this.clear();
            }
        };
    }


    //-------------------------------------------------------------
    // Implementation - DirtyableDBObject
    //-------------------------------------------------------------

    @Override
    public boolean isDirty() {
        return !addedOrUpdatedKeys.isEmpty() || !removedKeys.isEmpty() || getDirtyKeys().hasNext();
    }


    @Override
    public void markPersisted() {
        addedOrUpdatedKeys.clear();
        removedKeys.clear();

        for (Object o : delegate.entrySet()) {
            Entry entry = (Entry) o;

            //noinspection SuspiciousMethodCalls
            if (addedOrUpdatedKeys.contains(entry.getKey())) {
                continue;
            }

            if (entry.getValue() instanceof DirtyableDBObject) {
                DirtyableDBObject dirtyableDBObject = (DirtyableDBObject) entry.getValue();

                dirtyableDBObject.markPersisted();
            }
        }

        persisted = true;
    }


    @Override
    public boolean isPersisted() {
        return persisted;
    }


    @Override
    public Iterator<String> getDirtyKeys() {
        return new Iterator<String>() {
            private Iterator<Entry<String, Object>> entries = entrySet().iterator();
            private Entry<String, Object> entry;

            @Override
            public boolean hasNext() {
                while (entries.hasNext()) {
                    entry = entries.next();

                    // At this point, every value in the map is either a DirtyableDBObject or an immutable
                    // type (such as String).  The exception is the byte[] type, which doesn't get converted and
                    // we don't know if it changed, so we'll assume it's dirty.
                    if (addedOrUpdatedKeys.contains(entry.getKey())
                        || (entry.getValue() instanceof DirtyableDBObject
                            && (((DirtyableDBObject) entry.getValue()).isDirty() || !((DirtyableDBObject) entry.getValue()).isPersisted()))
                        || entry.getValue() instanceof byte[]) {
                        return true;
                    }
                }

                return false;
            }


            @Override
            public String next() {
                return entry.getKey();
            }


            @Override
            public void remove() {
                throw new JeppettoException("Can't remove items from dirtyKeys");
            }
        };
    }


    //-------------------------------------------------------------
    // Implementation - DBObject
    //-------------------------------------------------------------

    @Override
    public void markAsPartialObject() {
        throw new JeppettoException("Can't mark DirtyableDBObjectMap as partial");
    }


    @Override
    public boolean isPartialObject() {
        return false;
    }


    @Override
    public Object put(String key, Object value) {
        return delegate.put(key, value);
    }


    @Override
    public void putAll(BSONObject bsonObject) {
        for (Object o : bsonObject.toMap().entrySet()) {
            Entry entry = (Entry) o;

            delegate.put((String) entry.getKey(), entry.getValue());
        }
    }


    @Override
    public Object get(String key) {
        return get((Object) key);
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


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public Set getRemovedKeys() {
        return removedKeys;
    }
}
