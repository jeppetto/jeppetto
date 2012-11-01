package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.ReferenceSet;
import org.iternine.jeppetto.dao.mongodb.enhance.UpdateObject;
import org.iternine.jeppetto.enhance.Enhancer;

import com.mongodb.DBObject;


public class MongoDBReferenceSet<T>
        implements ReferenceSet<T> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private DBObject identifyingQuery;
    private Enhancer<T> updateObjectEnhancer;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public MongoDBReferenceSet(DBObject identifyingQuery, Enhancer<T> updateObjectEnhancer) {
        this.identifyingQuery = identifyingQuery;
        this.updateObjectEnhancer = updateObjectEnhancer;
    }


    //-------------------------------------------------------------
    // Implementation - ReferenceSet
    //-------------------------------------------------------------

    @Override
    public T getUpdateObject() {
        T updateObject = updateObjectEnhancer.newInstance();

        ((UpdateObject) updateObject).setPrefix("");    // Root object, so start with an empty prefix.

        return updateObject;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public DBObject getIdentifyingQuery() {
        return identifyingQuery;
    }
}
