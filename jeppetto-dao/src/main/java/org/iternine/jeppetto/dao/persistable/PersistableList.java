/*
 * Copyright (c) 2011-2017 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.persistable;


import org.iternine.jeppetto.dao.JeppettoException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;


@SuppressWarnings({ "unchecked" })
public class PersistableList
        implements Persistable, List {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private List delegate;
    private boolean rewrite = false;
    private Set<Integer> modifiedIndexes = new HashSet<Integer>();
    private int firstAppendedIndex;
    private boolean modifiableDelegate;
    private String storeIdentifier;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    /**
     * Default constructor that uses an ArrayList as the delegate and expects no non-Jeppetto access.
     */
    public PersistableList() {
        this(new ArrayList(), false);
    }


    /**
     * Constructor that uses an ArrayList with an initial capacity as the delegate and expects no non-Jeppetto access.
     */
    public PersistableList(int initialCapacity) {
        this(new ArrayList(initialCapacity), false);
    }


    /**
     * Constructor that takes is passed the delegate list along w/ an indication as to whether the
     * delegate is modifiable by code outside of Jeppetto.
     *
     * @param delegate the underlying List implementation
     * @param modifiableDelegate true if access is possible to the delegate by non-Jeppetto code
     */
    public PersistableList(List delegate, boolean modifiableDelegate) {
        this.delegate = delegate;
        this.firstAppendedIndex = delegate.size();
        this.modifiableDelegate = modifiableDelegate;
    }


    //-------------------------------------------------------------
    // Implementation - Persistable
    //-------------------------------------------------------------

    @Override
    public boolean __isDirty() {
        return rewrite
               || delegate.size() > firstAppendedIndex
               || !modifiedIndexes.isEmpty()
               || __getDirtyFields().hasNext();
    }


    @Override
    public void __markPersisted(String storeIdentifier) {
        for (Object object : delegate) {
            if (!(object instanceof Persistable)) {
                continue;
            }

            Persistable persistable = (Persistable) object;

            persistable.__markPersisted(storeIdentifier);
        }

        this.rewrite = false;
        this.modifiedIndexes.clear();
        this.firstAppendedIndex = delegate.size();
        this.storeIdentifier = storeIdentifier;
    }


    @Override
    public boolean __isPersisted(String storeIdentifier) {
        return storeIdentifier.equals(this.storeIdentifier);
    }


    @Override
    public Iterator<String> __getDirtyFields() {
        return new Iterator<String>() {
            private Iterator delegateIterator = delegate.iterator();
            private int i = -1;

            @Override
            public boolean hasNext() {
                while (delegateIterator.hasNext()) {
                    Object object = delegateIterator.next();

                    if (++i >= firstAppendedIndex
                        || modifiedIndexes.contains(i)
                        || !(object instanceof Persistable)
                        || ((Persistable) object).__isDirty()) {
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


    @Override
    public Object __getDelegate() {
        return delegate;
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
    public Object get(int index) {
        Object element = delegate.get(index);

        return element;

//        // TODO: revisit whether these semantics makes sense
//        if (element == null || element instanceof Persistable || DBObjectUtil.needsNoConversion(element.getClass())) {
//            return element;
//        }
//
//        Object converted = DBObjectUtil.toDBObject(element);
//        delegate.set(index, converted);
//
//        return converted;
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
    public boolean addAll(Collection elements) {
        return delegate.addAll(elements);
    }


    @Override
    public boolean remove(Object element) {
        boolean changed = delegate.remove(element);

        rewrite |= changed;

        return changed;
    }


    @Override
    public boolean removeAll(Collection elements) {
        boolean changed = delegate.removeAll(elements);

        rewrite |= changed;

        return changed;
    }


    @Override
    public boolean retainAll(Collection elements) {
        boolean changed = delegate.retainAll(elements);

        rewrite |= changed;

        return changed;
    }


    @Override
    public boolean contains(Object element) {
        return delegate.contains(element);
    }


    @Override
    public boolean containsAll(Collection elements) {
        return delegate.containsAll(elements);
    }


    @Override
    public int indexOf(Object element) {
        return delegate.indexOf(element);
    }


    @Override
    public int lastIndexOf(Object element) {
        return delegate.lastIndexOf(element);
    }


    @Override
    public Iterator iterator() {
        return listIterator(0);
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

                return next;

//                if (next == null || next instanceof Persistable || DBObjectUtil.needsNoConversion(next.getClass())) {
//                    return next;
//                }
//
//                Object converted = DBObjectUtil.toDBObject(next);
//                delegate.set(modifiableIndex, converted);
//
//                return converted;
            }


            @Override
            public boolean hasPrevious() {
                return delegateIterator.hasPrevious();
            }


            @Override
            public Object previous() {
                modifiableIndex = delegateIterator.previousIndex();
                Object previous = delegateIterator.previous();

                return previous;

//                if (previous == null || previous instanceof Persistable || DBObjectUtil.needsNoConversion(previous.getClass())) {
//                    return previous;
//                }
//
//                Object converted = DBObjectUtil.toDBObject(previous);
//                delegate.set(modifiableIndex, converted);
//
//                return converted;
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


    @Override
    public int size() {
        return delegate.size();
    }


    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }


    @Override
    public Object[] toArray() {
        // TODO: Convert to ...?
        return delegate.toArray();
    }


    @Override
    public Object[] toArray(Object[] objects) {
        // TODO: Convert to ...?
        return delegate.toArray(objects);
    }


    @Override
    public void clear() {
        rewrite = true;

        delegate.clear();
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public boolean isRewrite() {
        return rewrite;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof List)) {
            return false;
        }

        List thatList = o instanceof PersistableList ? ((PersistableList) o).delegate : (List) o;

        return delegate.equals(thatList);
    }


    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
