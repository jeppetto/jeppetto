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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.iternine.jeppetto.dao.mongodb.enhance.UpdateOperation.*;


public class UpdateSet
        implements Set, UpdateObject {

    //-------------------------------------------------------------
    // Constants - Private
    //-------------------------------------------------------------

    private static final String EACH = "$each";


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String prefix;
    private UpdateOperation operation;
    private Set<Object> updates = new HashSet<Object>();
    private boolean clear = false;


    //-------------------------------------------------------------
    // Implementation - Set
    //-------------------------------------------------------------

    @Override
    public boolean add(Object element) {
        verifyOperation($addToSet);

        updates.add(DBObjectUtil.toDBObject(element));

        return true;
    }


    @Override
    public boolean addAll(Collection collection) {
        verifyOperation($addToSet);

        for (Object element : collection) {
            updates.add(DBObjectUtil.toDBObject(element));
        }

        return true;
    }


    @Override
    public boolean remove(Object element) {
        if (clear) {
            throw new JeppettoException("remove() after clear() not supported");
        }

        verifyOperation($pullAll);

        updates.add(DBObjectUtil.toDBObject(element));

        return true;
    }


    @Override
    public boolean removeAll(Collection collection) {
        if (clear) {
            throw new JeppettoException("removeAll() after clear() not supported");
        }

        verifyOperation($pullAll);

        for (Object element : collection) {
            updates.add(DBObjectUtil.toDBObject(element));
        }

        return true;
    }


    @Override
    public void clear() {
        clear = true;
        operation = null;
        updates.clear();
    }


    @Override
    public boolean retainAll(Collection collection) {
        throw new JeppettoException("An UpdateSet does not support query operations.");
    }


    @Override
    public int size() {
        throw new JeppettoException("An UpdateSet does not support query operations.");
    }


    @Override
    public boolean isEmpty() {
        throw new JeppettoException("An UpdateSet does not support query operations.");
    }


    @Override
    public boolean contains(Object o) {
        throw new JeppettoException("An UpdateSet does not support query operations.");
    }


    @Override
    public Iterator iterator() {
        throw new JeppettoException("An UpdateSet does not support query operations.");
    }


    @Override
    public Object[] toArray() {
        throw new JeppettoException("An UpdateSet does not support query operations.");
    }


    @Override
    public Object[] toArray(Object[] objects) {
        throw new JeppettoException("An UpdateSet does not support query operations.");
    }


    @Override
    public boolean containsAll(Collection objects) {
        throw new JeppettoException("An UpdateSet does not support query operations.");
    }


    //-------------------------------------------------------------
    // Implementation - UpdateObject
    //-------------------------------------------------------------

    @Override
    public DBObject getUpdateClause() {
        if (clear) {
            // If new items were added, they'll be placed on the updateList.  Either way We use $set to ensure old
            // values are removed from the underlying store.
            BasicDBList updateList = new BasicDBList();
            updateList.addAll(updates);

            return new BasicDBObject($set.name(), new BasicDBObject(getNameFromPrefix(),  updateList));
        } else {
            if (updates.isEmpty()) {
                return new BasicDBObject();     // If there are no updates, return an empty DBObject
            } else {
                BasicDBList updateList = new BasicDBList();
                updateList.addAll(updates);
                
                if (operation.equals($addToSet)) {
                    return new BasicDBObject($addToSet.name(), new BasicDBObject(getNameFromPrefix(), new BasicDBObject(EACH, updateList)));
                } else {
                    return new BasicDBObject(operation.name(), new BasicDBObject(getNameFromPrefix(), updateList));
                }
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

    private void verifyOperation(UpdateOperation operation) {
        if (this.operation == null) {
            this.operation = operation;
        } else if (this.operation != operation) {
            throw new JeppettoException("Can't switch operation type to '" + operation + "'.  '"
                                        + this.operation + "' is already in use.");
        }
    }


    private String getNameFromPrefix() {
        return prefix.substring(0, prefix.length() - 1);
    }
}
