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

package org.iternine.jeppetto.dao.dirtyable;


import org.iternine.jeppetto.dao.JeppettoException;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class DirtyableMap
        implements Dirtyable, Map<String, Object> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Map<String, Object> delegate;
    private Set<String> addedOrUpdatedKeys = new HashSet<String>();
    private Set<String> removedKeys = new HashSet<String>();
    private String storeIdentifier;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DirtyableMap() {
        this(new HashMap<String, Object>());
    }


    public DirtyableMap(int initialCapacity) {
        this(new HashMap<String, Object>(initialCapacity));
    }


    public DirtyableMap(Map<String, Object> delegate) {
        this.delegate = delegate;
    }


    //-------------------------------------------------------------
    // Implementation - DirtyableDBObject
    //-------------------------------------------------------------

    @Override
    public boolean isDirty() {
        return !addedOrUpdatedKeys.isEmpty() || !removedKeys.isEmpty() || getDirtyFields().hasNext();
    }


    @Override
    public void markPersisted(String storeIdentifier) {
        addedOrUpdatedKeys.clear();
        removedKeys.clear();

        for (Object o : delegate.entrySet()) {
            Map.Entry entry = (Map.Entry) o;

            //noinspection SuspiciousMethodCalls
            if (addedOrUpdatedKeys.contains(entry.getKey())) {
                continue;
            }

            if (entry.getValue() instanceof Dirtyable) {
                Dirtyable dirtyableDBObject = (Dirtyable) entry.getValue();

                dirtyableDBObject.markPersisted(storeIdentifier);
            }
        }

        this.storeIdentifier = storeIdentifier;
    }


    @Override
    public boolean isPersisted(String storeIdentifier) {
        return storeIdentifier.equals(this.storeIdentifier);
    }


    @Override
    public Iterator<String> getDirtyFields() {
        return new Iterator<String>() {
            private Iterator<Map.Entry<String, Object>> entries = entrySet().iterator();
            private Map.Entry<String, Object> entry;

            @Override
            public boolean hasNext() {
                while (entries.hasNext()) {
                    entry = entries.next();

                    // At this point, every value in the map is either a DirtyableDBObject or an immutable
                    // type (such as String).  The exception is the byte[] type, which doesn't get converted and
                    // we don't know if it changed, so we'll assume it's dirty.
                    if (addedOrUpdatedKeys.contains(entry.getKey())
                        || (entry.getValue() instanceof Dirtyable
                            && (((Dirtyable) entry.getValue()).isDirty()
                                || !((Dirtyable) entry.getValue()).isPersisted(storeIdentifier)))
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


    @Override
    public Object getDelegate() {
        return delegate;
    }


    //-------------------------------------------------------------
    // Implementation - Map
    //-------------------------------------------------------------

    @Override
    public Object put(String key, Object value) {
        addedOrUpdatedKeys.add(key);

        if (removedKeys.size() > 0) {
            removedKeys.remove(key);
        }

        return delegate.put(key, value);
    }


    @Override
    public Object remove(Object key) {
        String stringKey = (String) key;
        Object result = delegate.remove(stringKey);

        if (result != null) {
            removedKeys.add(stringKey);

            if (addedOrUpdatedKeys.size() > 0) {
                addedOrUpdatedKeys.remove(stringKey);
            }
        }

        return result;
    }


    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        for (Map.Entry<? extends String, ? extends Object> entry : m.entrySet()) {
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
                        DirtyableMap.this.remove(currentKey);
                    }
                };
            }


            @Override
            public int size() {
                return delegate.size();
            }


            @Override
            public boolean remove(Object o) {
                return DirtyableMap.this.remove(o) != null;
            }


            @Override
            public void clear() {
                DirtyableMap.this.clear();
            }
        };
    }
}
