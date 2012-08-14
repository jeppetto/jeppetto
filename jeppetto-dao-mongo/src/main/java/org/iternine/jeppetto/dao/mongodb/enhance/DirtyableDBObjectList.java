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

import com.mongodb.DBCollection;
import org.bson.BSONObject;
import org.bson.util.StringRangeSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
    private Set<Integer> modifiedIndexes = new HashSet<Integer>();
    private int firstAppendedIndex;
    private boolean modifiableDelegate;
    private DBCollection persistentCollection;


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

        delegate.add(index, element);
    }


    @Override
    public boolean addAll(int index, Collection elements) {
        rewrite |= index < delegate.size();

        return delegate.addAll(index, elements);
    }


    @Override
    public Object remove(int index) {
        Object removed = delegate.remove(index);

        rewrite |= index < firstAppendedIndex;

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
        if (index < firstAppendedIndex) {
            modifiedIndexes.add(index);
        }

        return delegate.set(index, element);
    }


    @Override
    public boolean add(Object element) {
        return delegate.add(element);
    }


    @Override
    public boolean remove(Object object) {
        boolean changed = delegate.remove(object);

        rewrite |= changed;

        return changed;
    }


    @Override
    public boolean addAll(Collection elements) {
        return delegate.addAll(elements);
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
        return listIterator(0);
    }


    @Override
    public Object[] toArray() {
        // TODO: Convert to DirtyableDBObjects
        return delegate.toArray();
    }


    @Override
    public Object[] toArray(Object[] objects) {
        // TODO: Convert to DirtyableDBObjects
        return delegate.toArray(objects);
    }


    @Override
    public boolean containsAll(Collection objects) {
        return delegate.containsAll(objects);
    }


    @Override
    public Object get(int index) {
        Object element = delegate.get(index);

        if (element == null || element instanceof DirtyableDBObject || DBObjectUtil.needsNoConversion(element.getClass())) {
            return element;
        }

        // TODO: revisit whether these semantics makes sense
        Object converted = DBObjectUtil.toDBObject(element);
        delegate.set(index, converted);

        return converted;
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
        return listIterator(0);
    }


    @Override
    public ListIterator listIterator(final int index) {
        return new ListIterator() {
            private ListIterator delegateIterator = delegate.listIterator(index);
            private int modifiableIndex = -1;

            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }


            @Override
            public Object next() {
                modifiableIndex = delegateIterator.nextIndex();
                Object next = delegateIterator.next();

                if (next instanceof DirtyableDBObject || DBObjectUtil.needsNoConversion(next.getClass())) {
                    return next;
                }

                Object converted = DBObjectUtil.toDBObject(next);
                delegate.set(modifiableIndex, converted);

                return converted;
            }


            @Override
            public boolean hasPrevious() {
                return delegateIterator.hasPrevious();
            }


            @Override
            public Object previous() {
                modifiableIndex = delegateIterator.previousIndex();
                Object previous = delegateIterator.previous();

                if (previous instanceof DirtyableDBObject || DBObjectUtil.needsNoConversion(previous.getClass())) {
                    return previous;
                }

                Object converted = DBObjectUtil.toDBObject(previous);
                delegate.set(modifiableIndex, converted);

                return converted;
            }


            @Override
            public int nextIndex() {
                return delegateIterator.nextIndex();
            }


            @Override
            public int previousIndex() {
                return delegateIterator.previousIndex();
            }


            @Override
            public void remove() {
                delegateIterator.remove();

                rewrite |= modifiableIndex < firstAppendedIndex;
            }


            @Override
            public void set(Object o) {
                delegateIterator.set(o);

                // If items >= firstAppendedIndex are removed, the modifiedIndexes value will still be correct.
                // If items below it are removed, rewrite will be set to true and this will be ignored.
                modifiedIndexes.add(modifiableIndex);
            }


            @Override
            public void add(Object o) {
                throw new UnsupportedOperationException();
            }
        };
    }


    @Override
    public List subList(int fromIndex, int toIndex) {
        return delegate.subList(fromIndex, toIndex);    // TODO: Need to track changes in the sublist
    }


    //-------------------------------------------------------------
    // Implementation - DirtyableDBObject
    //-------------------------------------------------------------

    @Override
    public boolean isDirty() {
        return rewrite || delegate.size() > firstAppendedIndex || !modifiedIndexes.isEmpty() || getDirtyKeys().hasNext();
    }


    @Override
    public void markPersisted(DBCollection dbCollection) {
        for (Object object : delegate) {
            if (!(object instanceof DirtyableDBObject)) {
                continue;
            }

            DirtyableDBObject dirtyableDBObject = (DirtyableDBObject) object;

            dirtyableDBObject.markPersisted(dbCollection);
        }

        rewrite = false;
        modifiedIndexes.clear();
        firstAppendedIndex = delegate.size();
        persistentCollection = dbCollection;
    }


    @Override
    public boolean isPersisted(DBCollection dbCollection) {
        return dbCollection.equals(persistentCollection);
    }


    @Override
    public Iterator<String> getDirtyKeys() {
        return new Iterator<String>() {
            private Iterator delegateIterator = delegate.iterator();
            private int i = -1;

            @Override
            public boolean hasNext() {
                while (delegateIterator.hasNext()) {
                    Object object = delegateIterator.next();

                    if (++i >= firstAppendedIndex
                        || modifiedIndexes.contains(i)
                        || !(object instanceof DirtyableDBObject)
                        || ((DirtyableDBObject) object).isDirty()) {
                        return true;
                    }
                }

                return false;
            }


            @Override
            public String next() {
                return Integer.toString(i);
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
        throw new JeppettoException("Can't mark DirtyableDBObjectList as partial");
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
        return get(getNonNegativeInt(s));
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
        return rewrite;
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
