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

package org.iternine.jeppetto.dao.mongodb.enhance;


import org.iternine.jeppetto.dao.JeppettoException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static org.iternine.jeppetto.dao.mongodb.enhance.UpdateOperation.*;


public class UpdateList
        implements List, UpdateObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String prefix;
    private UpdateOperation operation;
    private DBObject updates;
    private boolean clear = false;


    //-------------------------------------------------------------
    // Implementation - List
    //-------------------------------------------------------------

    @Override
    public Object set(int index, Object element) {
        if (clear) {
            throw new JeppettoException("set() after clear() not supported");
        }

        verifyOperation($set, BasicDBObject.class);

        return updates.put(prefix + index, DBObjectUtil.toDBObject(element));
    }


    @Override
    public boolean add(Object element) {
        verifyOperation($pushAll, BasicDBList.class);

        ((BasicDBList) updates).add(DBObjectUtil.toDBObject(element));

        return true;
    }


    @Override
    public boolean addAll(Collection collection) {
        verifyOperation($pushAll, BasicDBList.class);

        for (Object element : collection) {
            ((BasicDBList) updates).add(DBObjectUtil.toDBObject(element));
        }

        return true;
    }


    @Override
    public boolean remove(Object element) {
        if (clear) {
            throw new JeppettoException("remove() after clear() not supported");
        }

        verifyOperation($pullAll, BasicDBList.class);

        ((BasicDBList) updates).add(DBObjectUtil.toDBObject(element));

        return true;
    }


    @Override
    public boolean removeAll(Collection collection) {
        if (clear) {
            throw new JeppettoException("removeAll() after clear() not supported");
        }

        verifyOperation($pullAll, BasicDBList.class);

        for (Object element : collection) {
            ((BasicDBList) updates).add(DBObjectUtil.toDBObject(element));
        }

        return true;
    }


    @Override
    public void clear() {
        clear = true;
        operation = null;
        updates = new BasicDBList();    // When clear() is called, List-types become the only supported form.
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
    public Object remove(int index) {
        throw new JeppettoException("Can't remove() an item to an UpdateList at a specific index");
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
    // Implementation - UpdateObject
    //-------------------------------------------------------------

    @Override
    public DBObject getUpdateClause() {
        if (clear) {
            // If clear() was called, updates will either be an empty BasicDBList or items added
            return new BasicDBObject($set.name(), new BasicDBObject(getNameFromPrefix(), updates));
        } else {
            if (operation == null) {
                return new BasicDBObject();     // If there are no updates, return an empty DBObject
            } else if (operation == UpdateOperation.$set) {
                return new BasicDBObject(operation.name(), updates);
            } else {
                return new BasicDBObject(operation.name(), new BasicDBObject(getNameFromPrefix(), updates));
            }
        }
    }


    @Override
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }


    // Temp hack
    @Override
    public Map<String, Object> __getUpdates() {
        return null;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void verifyOperation(UpdateOperation operation, Class<? extends DBObject> updatesClass) {
        if (this.operation == null) {
            this.operation = operation;

            try {
                this.updates = updatesClass.newInstance();
            } catch (Exception e) {
                throw new JeppettoException(e);
            }
        } else if (this.operation != operation) {
            throw new JeppettoException("Can't switch operation type to '" + operation + "'.  '"
                                        + this.operation + "' is already in use.");
        }
    }


    private String getNameFromPrefix() {
        return prefix.substring(0, prefix.length() - 1);
    }
}
