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
import org.bson.util.StringRangeSet;

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
public class DirtyableDBObjectList
        implements List, DirtyableDBObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private List delegate;
    private boolean rewrite = false;
    private int firstAppendedIndex;
    private boolean modifiableDelegate;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    /**
     * Default constructor that uses an ArrayList as the delegate and expects no non-Jeppetto
     * access.
     */
    public DirtyableDBObjectList() {
        this(new ArrayList(), false);
    }


    /**
     * Constructor that takes is passed the delegate list along w/ an indication as to
     * whether the delegate is modifiable by code outside of Jeppetto.
     *
     * @param delegate the underlying List implementation
     * @param modifiableDelegate true if access is possible to the delegate by non-Jeppetto code
     */
    public DirtyableDBObjectList(List delegate, boolean modifiableDelegate) {
        this.delegate = delegate;
        this.firstAppendedIndex = delegate.size();
        this.modifiableDelegate = modifiableDelegate;
    }


    //-------------------------------------------------------------
    // Implementation - List
    //-------------------------------------------------------------

    @Override
    public void add(int index, Object element) {
        rewrite |= index < delegate.size();

        Object convertedElement = DBObjectUtil.toDBObject(element);
        delegate.add(index, convertedElement);
    }


    @Override
    public boolean addAll(int index, Collection elements) {
        rewrite |= index < delegate.size();

        Collection convertedElements = new ArrayList();
        for (Object element : elements) {
            convertedElements.add(DBObjectUtil.toDBObject(element));
        }

        return delegate.addAll(index, convertedElements);
    }


    @Override
    public Object remove(int index) {
        Object removed = delegate.remove(index);

        rewrite = true;

        return removed;
    }


    @Override
    public boolean removeAll(Collection collection) {
        boolean changed = delegate.removeAll(collection);

        rewrite |= changed;

        return changed;
    }


    @Override
    public Object set(int index, Object element) {
        rewrite = true;     // TODO:  tracked modified index value instead?

        return delegate.set(index, element);
    }


    @Override
    public boolean add(Object element) {
        Object convertedElement = DBObjectUtil.toDBObject(element);

        return delegate.add(convertedElement);
    }


    @Override
    public boolean remove(Object object) {
        boolean changed = delegate.remove(object);

        rewrite |= changed;

        return changed;
    }


    @Override
    public boolean addAll(Collection elements) {
        Collection convertedElements = new ArrayList();
        for (Object element : elements) {
            convertedElements.add(DBObjectUtil.toDBObject(element));
        }

        return delegate.addAll(convertedElements);
    }


    @Override
    public boolean retainAll(Collection collection) {
        boolean changed = delegate.retainAll(collection);

        rewrite |= changed;

        return changed;
    }


    @Override
    public void clear() {
        rewrite = true;

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
        return delegate.iterator(); // TODO: if iterator.remove() is called, need to track...
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
    public Object get(int index) {
        return delegate.get(index);
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
    public ListIterator listIterator(int index) {
        return delegate.listIterator(index);    // TODO: if listIterator.remove() is called, need to track...
    }


    @Override
    public List subList(int fromIndex, int toIndex) {
        return delegate.subList(fromIndex, toIndex);
    }


    //-------------------------------------------------------------
    // Implementation - DirtyableDBObject
    //-------------------------------------------------------------

    @Override
    public boolean isDirty() {
        // TODO: walk items (stop at appended size)
        return rewrite || delegate.size() > firstAppendedIndex;
    }


    @Override
    public void markPersisted() {
        int i = 0;

        for (Object object : delegate) {
            if (!(object instanceof DirtyableDBObject)) {
                break;  // We assume this means the list contains all non-DirtyableDBObjects so can break out
            }

            DirtyableDBObject dirtyableDBObject = (DirtyableDBObject) object;

            dirtyableDBObject.markPersisted();

            if (++i >= firstAppendedIndex) {
                break;
            }
        }

        rewrite = false;
        firstAppendedIndex = delegate.size();
    }


    @Override
    public boolean isPersisted() {
        throw new RuntimeException("Can't determine persisted state.");
    }


    @Override
    public Set<String> getDirtyKeys() {
        Set<String> dirtyKeys = new LinkedHashSet<String>();
        int i = 0;

        for (Object object : delegate) {
            DirtyableDBObject dirtyableDBObject = (DirtyableDBObject) object;

            if (dirtyableDBObject.isDirty()) {
                dirtyKeys.add(Integer.toString(i));
            }

            if (++i >= firstAppendedIndex) {
                break;
            }
        }

        return dirtyKeys;
    }


    //-------------------------------------------------------------
    // Implementation - DBObject
    //-------------------------------------------------------------

    @Override
    public void markAsPartialObject() {
        throw new RuntimeException("Can't mark DirtyableDBObjectList as partial");
    }


    @Override
    public boolean isPartialObject() {
        return false;
    }


    @Override
    public Set<String> keySet() {
        return new StringRangeSet(delegate.size());
    }


    @Override
    public boolean containsField(String s) {
        return getNonNegativeInt(s) < size();
    }


    @Override
    public boolean containsKey(String s) {
        return containsField(s);
    }


    @Override
    public Object removeField(String s) {
        int i = getNonNegativeInt(s);

        if (i >= size()) {
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
        int i = getNonNegativeInt(s);

        if (i >= size()) {
            return null;
        }

        Object result = delegate.get(i);

        if (!(result instanceof DirtyableDBObject)) {
            result = DBObjectUtil.toDBObject(result);

            delegate.set(i, result);
        }

        return result;
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
        int i = getNonNegativeInt(s);

        while (i > size()) {
            delegate.add(null);
        }

        delegate.add(v);

        return v;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public boolean isRewrite() {
        if (rewrite) {
            return true;
        }

        int i = 0;

        for (Object object : delegate) {
            DirtyableDBObject dirtyableDBObject = (DirtyableDBObject) object;

            if (dirtyableDBObject.isDirty()) {
                return true;
            }

            if (++i >= firstAppendedIndex) {
                break;
            }
        }

        return false;
    }


    public boolean hasAppendedItems() {
        return delegate.size() > firstAppendedIndex;
    }


    public List getAppendedItems() {
        return delegate.subList(firstAppendedIndex, delegate.size());
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private int getNonNegativeInt(String s) {
        int i;

        try {
            i = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unable to handle non-numeric value " + s);
        }

        if (i < 0) {
            throw new IllegalArgumentException(s + " is an invalid index value");
        }

        return i;
    }
}
