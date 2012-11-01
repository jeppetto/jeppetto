package org.iternine.jeppetto.dao.mongodb.enhance;


import org.iternine.jeppetto.dao.JeppettoException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.iternine.jeppetto.dao.mongodb.enhance.UpdateOperation.*;


public class UpdateMap
        implements Map, UpdateObject{

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String prefix;
    private UpdateOperation operation;
    private BasicDBObject updates = new BasicDBObject();
    private boolean clear = false;


    //-------------------------------------------------------------
    // Implementation - Map
    //-------------------------------------------------------------

    @Override
    // TODO: what happens if pushAll and key already exists?
    public Object put(Object key, Object value) {
        verifyOperation($set);

        return updates.put(getKeyForUpdate(key), DBObjectUtil.toDBObject(value));
    }


    @Override
    public void putAll(Map m) {
        verifyOperation($set);

        for (Object o : m.entrySet()) {
            Entry entry = (Entry) o;

            updates.put(getKeyForUpdate(entry.getKey()), DBObjectUtil.toDBObject(entry.getValue()));
        }
    }


    @Override
    public Object remove(Object key) {
        if (clear) {
            throw new JeppettoException("remove() after clear() not supported");
        }

        verifyOperation($unset);

        return updates.put(prefix + key, 1);
    }


    @Override
    public void clear() {
        clear = true;
        operation = null;
        updates.clear();
    }


    @Override
    public int size() {
        throw new JeppettoException("An UpdateMap does not support query operations.");
    }


    @Override
    public boolean isEmpty() {
        throw new JeppettoException("An UpdateMap does not support query operations.");
    }


    @Override
    public boolean containsKey(Object key) {
        throw new JeppettoException("An UpdateMap does not support query operations.");
    }


    @Override
    public boolean containsValue(Object value) {
        throw new JeppettoException("An UpdateMap does not support query operations.");
    }


    @Override
    public Object get(Object key) {
        throw new JeppettoException("An UpdateMap does not support query operations.");
    }


    @Override
    public Set keySet() {
        throw new JeppettoException("An UpdateMap does not support query operations.");
    }


    @Override
    public Collection values() {
        throw new JeppettoException("An UpdateMap does not support query operations.");
    }


    @Override
    public Set entrySet() {
        throw new JeppettoException("An UpdateMap does not support query operations.");
    }


    //-------------------------------------------------------------
    // Implementation - UpdateObject
    //-------------------------------------------------------------

    @Override
    public DBObject getUpdateClause() {
        if (clear) {
            // If clear() was called, updates will either be an empty Map or added items with prefix-less keys
            return new BasicDBObject($set.name(), new BasicDBObject(getNameFromPrefix(), updates));
        } else if (operation == null) {
            return new BasicDBObject();     // If there are no updates, return an empty DBObject
        } else {
            return new BasicDBObject(operation.name(), updates);
        }
    }


    @Override
    public void setPrefix(String prefix) {
        this.prefix = prefix;
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


    private String getKeyForUpdate(Object key) {
        // If 'clear', we'll be storing the entire update list as a map, not on a key-by-key basis
        return clear ? (String) key : prefix + key;
    }


    private String getNameFromPrefix() {
        return prefix.substring(0, prefix.length() - 1);
    }
}