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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


@SuppressWarnings( { "unchecked" })
public class DirtyableDBObjectSet
        implements Set, DirtyableDBObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Set delegate;
    private boolean dirty = false;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DirtyableDBObjectSet() {
        this.delegate = new HashSet();
    }


    public DirtyableDBObjectSet(Set delegate) {
        this.delegate = delegate;
    }


    //-------------------------------------------------------------
    // Implementation - Set
    //-------------------------------------------------------------

    @Override
    public int size() {
        return delegate.size();
    }


    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }


    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }


    @Override
    public Iterator iterator() {
        return delegate.iterator(); // TODO: make an immutable iterator or else track dirty state from it
    }


    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }


    @Override
    public Object[] toArray(Object[] objects) {
        return delegate.toArray(objects);
    }


    @Override
    public boolean add(Object o) {
        boolean changed = delegate.add(o);

        dirty |= changed;

        return changed;
    }


    @Override
    public boolean remove(Object o) {
        boolean changed = delegate.remove(o);

        dirty |= changed;

        return changed;
    }


    @Override
    public boolean containsAll(Collection collection) {
        return delegate.containsAll(collection);
    }


    @Override
    public boolean addAll(Collection collection) {
        boolean changed = delegate.addAll(collection);

        dirty |= changed;

        return changed;
    }


    @Override
    public boolean retainAll(Collection collection) {
        boolean changed = delegate.retainAll(collection);

        dirty |= changed;

        return changed;
    }


    @Override
    public boolean removeAll(Collection collection) {
        boolean changed = delegate.removeAll(collection);

        dirty |= changed;

        return changed;
    }


    @Override
    public void clear() {
        if (!isEmpty()) {
            dirty = true;
        }

        delegate.clear();
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
        throw new RuntimeException("Can't mark DirtyableDBObjectSet as partial");
    }


    @Override
    public boolean isPartialObject() {
        return false;
    }


    @Override
    public Set<String> keySet() {
        Set<String> keys = new LinkedHashSet<String>();

        for (Object o : delegate) {
            keys.add(o.toString());
        }

        return keys;
    }


    @Override
    public boolean containsField(String s) {
        for (Object o : delegate) {
            if (o.toString().equals(s)) {
                return true;
            }
        }

        return false;
    }


    @Override
    public boolean containsKey(String s) {
        return containsField(s);
    }


    @Override
    public Object removeField(String s) {
        for (Iterator iterator = delegate.iterator(); iterator.hasNext(); ) {
            Object o = iterator.next();

            if (o.toString().equals(s)) {
                iterator.remove();
                dirty = true;

                return o;
            }
        }

        return null;
    }


    @Override
    public Map toMap() {
        Map result = new HashMap();

        for (String key : keySet()) {
            result.put(key, get(key));
        }

        return result;
    }


    @Override
    public Object get(String s) {
        for (Object o : delegate) {
            if (o.toString().equals(s)) {
                return o;
            }
        }

        return null;
    }


    @Override
    public void putAll(Map m) {
        for (Map.Entry entry : (Set<Map.Entry>) m.entrySet()) {
            put(entry.getKey().toString(), entry.getValue() );
        }
    }


    @Override
    public void putAll(BSONObject o) {
        for (String k : o.keySet()) {
            put(k, o.get(k));
        }
    }


    @Override
    public Object put(String s, Object v) {
        add(v);

        return v;
    }
}
