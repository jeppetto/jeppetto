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

package org.jeppetto.dao.mongodb.enhance;


import com.mongodb.DBObject;
import org.bson.BSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;


@SuppressWarnings({ "unchecked" })
public class DirtyableList
        implements List, Dirtyable, DBObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private boolean dirty;
    private List delegate;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DirtyableList() {
        this.delegate = new ArrayList();
    }


    public DirtyableList(List delegate) {
        this.delegate = delegate;
    }


    //-------------------------------------------------------------
    // Implementation - List
    //-------------------------------------------------------------

    @Override
    public void add(int index, Object element) {
        dirty = true;

        delegate.add(index, element);
    }


    @Override
    public boolean addAll(int index, Collection elements) {
        dirty = true;

        return delegate.addAll(index, elements);
    }


    @Override
    public Object remove(int index) {
        Object removed = delegate.remove(index);

        dirty = true;

        return removed;
    }


    @Override
    public boolean removeAll(Collection collection) {
        boolean changed = delegate.removeAll(collection);

        dirty |= changed;

        return changed;
    }


    @Override
    public Object set(int index, Object element) {
        dirty = true;

        return delegate.set(index, element);
    }


    @Override
    public boolean add(Object element) {
        boolean changed = delegate.add(element);

        dirty |= changed;

        return changed;
    }


    @Override
    public boolean remove(Object object) {
        boolean changed = delegate.remove(object);

        dirty |= changed;

        return changed;
    }


    @Override
    public boolean addAll(Collection ts) {
        boolean changed = delegate.addAll(ts);

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
    public void clear() {
        if (!delegate.isEmpty()) {
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
    public boolean contains(Object o) {
        return delegate.contains(o);
    }


    @Override
    public Iterator iterator() {
        return delegate.iterator();
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
    public boolean containsAll(Collection objects) {
        return delegate.containsAll(objects);
    }


    @Override
    public Object get(int i) {
        return delegate.get(i);
    }


    @Override
    public int indexOf(Object o) {
        return delegate.indexOf(o);
    }


    @Override
    public int lastIndexOf(Object o) {
        return delegate.lastIndexOf(o);
    }


    @Override
    public ListIterator listIterator() {
        return delegate.listIterator();
    }


    @Override
    public ListIterator listIterator(int i) {
        return delegate.listIterator(i);
    }


    @Override
    public List subList(int i, int i1) {
        return delegate.subList(i, i1);
    }


    //-------------------------------------------------------------
    // Implementation - Dirtyable
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
        throw new RuntimeException("Can't mark DirtyableList as partial");
    }


    @Override
    public boolean isPartialObject() {
        return false;
    }


    @Override
    public Set<String> keySet() {
        Set<String> keys = new LinkedHashSet<String>();

        for (int i = 0; i < size(); i++) {
            keys.add(String.valueOf(i));
        }

        return keys;
    }


    @Override
    public boolean containsField(String s) {
        int i = getInt(s, false);

        return i >= 0 && i < size();
    }


    @Override
    public boolean containsKey(String s) {
        return containsField(s);
    }


    @Override
    public Object removeField(String s) {
        int i = getInt(s, true);

        if (i < 0 || i >= size()) {
            return null;
        }

        return remove(i);
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
        int i = getInt(s, true);

        if (i < 0 || i >= size()) {
            return null;
        }

        return delegate.get(i);
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
        int i = getInt(s, true);

        while (i >= size()) {
            delegate.add(null);
        }

        set(i, v);

        return v;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private int getInt(String s, boolean throwException) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            if (throwException) {
                throw new IllegalArgumentException("Unable to handle non-numeric value " + s);
            }

            return -1;
        }
    }
}
