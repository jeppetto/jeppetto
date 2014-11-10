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
