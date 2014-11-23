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

package org.iternine.jeppetto.dao.updateobject;


import org.iternine.jeppetto.dao.JeppettoException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;


public class UpdateList
        implements UpdateObject, List {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Map<String, Object> updates = new TreeMap<String, Object>();
    private boolean cleared = false;
    private String indexFormat;
    private int addIndex = 1000000000;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public UpdateList(String indexFormat) {
        this.indexFormat = indexFormat;
    }


    //-------------------------------------------------------------
    // Implementation - UpdateObject
    //-------------------------------------------------------------

    @Override
    public Map<String, Object> __getUpdates() {
        return updates;
    }


    //-------------------------------------------------------------
    // Implementation - List
    //-------------------------------------------------------------

    @Override
    public Object set(int index, Object element) {
        if (cleared) {
            throw new JeppettoException("set() after clear() not supported");
        }

        return updates.put(String.format(indexFormat, index), element);
    }


    @Override
    public boolean add(Object element) {
        updates.put(String.format(indexFormat, addIndex++), element);

        return true;
    }


    @Override
    public boolean addAll(Collection collection) {
        for (Object element : collection) {
            updates.put(String.format(indexFormat, addIndex++), element);
        }

        return true;
    }


    @Override
    public Object remove(int index) {
        return updates.put(String.format(indexFormat, index), null);
    }


    @Override
    public void clear() {
        cleared = true;
        updates.clear();
    }


    @Override
    public void add(int index, Object element) {
        throw new JeppettoException("Can't add() an item to an UpdateList at a specific index");
    }


    @Override
    public boolean addAll(int index, Collection elements) {
        throw new JeppettoException("Can't addAll() items to an UpdateList at a specific index");
    }


    @Override
    public boolean remove(Object element) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public boolean removeAll(Collection collection) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public boolean retainAll(Collection collection) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public int size() {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public boolean isEmpty() {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public boolean contains(Object o) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public Iterator iterator() {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public Object[] toArray() {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public Object[] toArray(Object[] objects) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public boolean containsAll(Collection objects) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public Object get(int index) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public int indexOf(Object o) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public int lastIndexOf(Object o) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public ListIterator listIterator() {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public ListIterator listIterator(final int index) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    @Override
    public List subList(int fromIndex, int toIndex) {
        throw new JeppettoException("An UpdateList does not support query operations.");
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public boolean wasCleared() {
        return cleared;
    }
}
