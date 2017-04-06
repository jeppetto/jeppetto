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

package org.iternine.jeppetto.dao.mongodb.enhance;


import org.iternine.jeppetto.dao.JeppettoException;

import org.bson.BSON;
import org.bson.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


/**
 * A DirtyableDBObjectSet uses two underlying collection objects to manage the semantics of this class.  The
 * first is a Set object which is used to ensure Set semantics are followed, the second is an underlying List,
 * provided by the base class, to ensure ordering and provide the lookup by index methods that DBObject
 * implementations require.
 */
@SuppressWarnings({ "unchecked" })
public class DirtyableDBObjectSet extends DirtyableDBObjectList
        implements Set {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Set delegate;

    private static Logger logger = LoggerFactory.getLogger(DirtyableDBObjectSet.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DirtyableDBObjectSet() {
        super();

        this.delegate = new HashSet();
    }


    /**
     *
     * @param delegate Set instance this object should delegate responsibility to
     * @param modifiableDelegate true if access is possible to the delegate by non-Jeppetto code
     */
    // TODO: Determine who calls and if we should have a DirtyableDBObjectSet(List delegate, boolean modi...)
    // TODO: Do we need to convert items in the delegate() to DirtyableDBObjects?
    public DirtyableDBObjectSet(Set delegate, boolean modifiableDelegate) {
        super(new ArrayList(delegate), modifiableDelegate);

        this.delegate = delegate;
    }


    //-------------------------------------------------------------
    // Implementation - Set
    //-------------------------------------------------------------

    @Override
    public int size() {
        return super.size();
    }


    @Override
    public boolean isEmpty() {
        return super.isEmpty();
    }


    @Override
    public boolean contains(Object element) {
        return delegate.contains(element);
    }


    @Override
    public Iterator iterator() {
        final Iterator iterator = super.iterator();

        return new Iterator() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }


            @Override
            public Object next() {
                return iterator.next();
            }


            @Override
            public void remove() {
                // TODO: Verify this will return the same 'next()'
                Object next = iterator.next();

                delegate.remove(next);

                iterator.remove();

                // TODO: Mark super as dirty?
            }
        };
    }


    @Override
    public Object[] toArray() {
        return super.toArray();
    }


    @Override
    public Object[] toArray(Object[] objects) {
        return super.toArray(objects);
    }


    @Override
    public boolean add(Object element) {
        if (delegate.add(element)) {
            super.add(element);

            return true;
        }

        return false;
    }


    @Override
    public boolean remove(Object element) {
        if (delegate.remove(element)) {
            super.remove(element); // TODO: verify remove?

            return true;
        }

        return false;
    }


    @Override
    public boolean containsAll(Collection collection) {
        return delegate.containsAll(collection);
    }


    @Override
    public boolean addAll(Collection elements) {
        boolean changed = false;

        for (Object element : elements) {
            changed |= add(element);
        }

        return changed;
    }


    @Override
    public boolean retainAll(Collection elements) {
        throw new JeppettoException("Not implemented");
    }


    @Override
    public boolean removeAll(Collection elements) {
        boolean changed = false;

        for (Object element : elements) {
            changed |= remove(element);
        }

        return changed;
    }


    @Override
    public void clear() {
        super.clear();
        delegate.clear();
    }


    //-------------------------------------------------------------
    // Implementation - DirtyableDBObject
    //-------------------------------------------------------------

    @Override
    public Object getDelegate() {
        return delegate;
    }


    //-------------------------------------------------------------
    // Implementation - DBObject
    //-------------------------------------------------------------

    @Override
    public Object removeField(String s) {
        Object o = super.removeField(s);

        if (o != null) {
            delegate.remove(o);
        }

        return o;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public Transformer getDecodingTransformer() {
        return new DecodingTransformer();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Set)) {
            return false;
        }

        Set thatSet = o instanceof DirtyableDBObjectSet ? ((DirtyableDBObjectSet) o).delegate : (Set) o;

        return delegate.equals(thatSet);
    }


    @Override
    public int hashCode() {
        return delegate.hashCode();
    }


    //-------------------------------------------------------------
    // Inner Class - DecodingTransformer
    //-------------------------------------------------------------

    class DecodingTransformer
            implements Transformer {

        //-------------------------------------------------------------
        // Implementation - Transformer
        //-------------------------------------------------------------

        @Override
        public Object transform(Object o) {
            if (o != DirtyableDBObjectSet.this) {
                return o;
            }

            Iterator iterator = iterator();

            while (iterator.hasNext()) {
                Object next = iterator.next();

                if (!delegate.add(next)) {
                    logger.error("Dropping object because it is already a member of the set.  Did you change the"
                                 + " equals() method and/or collection type?  Object = " + next);

                    DirtyableDBObjectSet.super.remove(next);
                }
            }

            BSON.removeDecodingHook(DirtyableDBObjectSet.class, this);

            return DirtyableDBObjectSet.this;
        }
    }
}
