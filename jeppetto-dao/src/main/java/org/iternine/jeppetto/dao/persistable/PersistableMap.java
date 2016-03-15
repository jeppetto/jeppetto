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

package org.iternine.jeppetto.dao.persistable;


import org.iternine.jeppetto.dao.JeppettoException;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class PersistableMap
        implements Persistable, Map<String, Object> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Map<String, Object> delegate;
    private Set<String> updatedKeys = new HashSet<String>();
    private String storeIdentifier;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public PersistableMap() {
        this(new HashMap<String, Object>());
    }


    public PersistableMap(int initialCapacity) {
        this(new HashMap<String, Object>(initialCapacity));
    }


    public PersistableMap(Map<String, Object> delegate) {
        this.delegate = delegate;
    }


    //-------------------------------------------------------------
    // Implementation - DirtyableDBObject
    //-------------------------------------------------------------

    @Override
    public boolean __isDirty() {
        return !updatedKeys.isEmpty() || __getDirtyFields().hasNext();
    }


    @Override
    public void __markPersisted(String storeIdentifier) {
        updatedKeys.clear();

        for (Object o : delegate.entrySet()) {
            Map.Entry entry = (Map.Entry) o;

            //noinspection SuspiciousMethodCalls
            if (updatedKeys.contains(entry.getKey())) {
                continue;
            }

            if (entry.getValue() instanceof Persistable) {
                Persistable persistable = (Persistable) entry.getValue();

                persistable.__markPersisted(storeIdentifier);
            }
        }

        this.storeIdentifier = storeIdentifier;
    }


    @Override
    public boolean __isPersisted(String storeIdentifier) {
        return storeIdentifier.equals(this.storeIdentifier);
    }


    @Override
    public Iterator<String> __getDirtyFields() {
        return new Iterator<String>() {
            private Iterator<String> keysIterator;
            private String key;

            {
                Set<String> allKeys = new HashSet<String>(delegate.keySet());
                allKeys.addAll(updatedKeys);

                this.keysIterator = allKeys.iterator();
            }

            @Override
            public boolean hasNext() {
                while (keysIterator.hasNext()) {
                    key = keysIterator.next();

                    if (updatedKeys.contains(key)) {
                        return true;
                    }

                    Object value = delegate.get(key);

                    if (value instanceof Persistable && (((Persistable) value).__isDirty()
                                                       || !((Persistable) value).__isPersisted(storeIdentifier))) {
                        return true;
                    }
                }

                return false;
            }


            @Override
            public String next() {
                return key;
            }


            @Override
            public void remove() {
                throw new JeppettoException("Can't remove items from dirtyFields");
            }
        };
    }


    @Override
    public Object __getDelegate() {
        return delegate;
    }


    //-------------------------------------------------------------
    // Implementation - Map
    //-------------------------------------------------------------

    @Override
    public Object put(String key, Object value) {
        updatedKeys.add(key);

        return delegate.put(key, value);
    }


    @Override
    public Object remove(Object key) {
        String stringKey = (String) key;
        Object result = delegate.remove(stringKey);

        if (result != null) {
            updatedKeys.add(stringKey);
        }

        return result;
    }


    @Override
    public void putAll(Map<? extends String, ?> m) {
        for (Map.Entry<? extends String, ?> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }


    @Override
    public void clear() {
        updatedKeys.clear();
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

        return value;

//        if (value == null || value instanceof DirtyableDBObject || DBObjectUtil.needsNoConversion(value.getClass())) {
//            return value;
//        }
//
//        // TODO: revisit whether these semantics makes sense
//        Object converted = DBObjectUtil.toDBObject(value);
//        delegate.put((String) key, converted);
//
//        return converted;
    }


    @Override
    public Set<String> keySet() {
        // TODO: allow key set to be modified
        return Collections.unmodifiableSet(delegate.keySet());
    }


    @Override
    public Collection<Object> values() {
        // TODO: allow values to be modified
        return Collections.unmodifiableCollection(delegate.values());
    }


    @Override
    public Set<Entry<String, Object>> entrySet() {
        return new AbstractSet<Entry<String, Object>>() {
            @Override
            public Iterator<Entry<String, Object>> iterator() {
                return new Iterator<Entry<String, Object>>() {
                    private Iterator<Entry<String, Object>> delegateIterator = delegate.entrySet().iterator();
                    private String currentKey;

                    @Override
                    public boolean hasNext() {
                        return delegateIterator.hasNext();
                    }


                    @Override
                    public Entry<String, Object> next() {
                        Entry<String, Object> current = delegateIterator.next();

                        return current;

//                        this.currentKey = current.getKey();
//                        final Object currentValue;
//
//                        if (current.getValue() instanceof DirtyableDBObject
//                            || current.getValue() == null
//                            || DBObjectUtil.needsNoConversion(current.getValue().getClass())) {
//                            currentValue = current.getValue();
//                        } else {
//                            currentValue = DBObjectUtil.toDBObject(current.getValue());
//
//                            delegate.put(currentKey, currentValue);
//                        }
//
//                        return new Map.Entry<String, Object>() {
//                            @Override
//                            public String getKey() {
//                                return currentKey;
//                            }
//
//
//                            @Override
//                            public Object getValue() {
//                                return currentValue;
//                            }
//
//
//                            @Override
//                            public Object setValue(Object value) {
//                                throw new UnsupportedOperationException();
//                            }
//                        };
                    }


                    @Override
                    public void remove() {
                        PersistableMap.this.remove(currentKey);
                    }
                };
            }


            @Override
            public int size() {
                return delegate.size();
            }


            @Override
            public boolean remove(Object o) {
                return PersistableMap.this.remove(o) != null;
            }


            @Override
            public void clear() {
                PersistableMap.this.clear();
            }
        };
    }
}
