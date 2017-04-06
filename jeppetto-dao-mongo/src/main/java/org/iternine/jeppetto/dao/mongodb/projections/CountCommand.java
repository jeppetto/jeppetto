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

package org.iternine.jeppetto.dao.mongodb.projections;


import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


/**
 * Counts documents which have a value for a given field.
 */
class CountCommand
        extends RowCountCommand {

    //-------------------------------------------------------------
    // Methods - Private - Static
    //-------------------------------------------------------------

    private static DBObject addFieldExistsCriterion(DBObject query, String field) {
        return new BasicDBObject(query.toMap()).append(field, new BasicDBObject("$exists", true));
    }


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------
    
    public CountCommand(DBObject query, String field) {
        super(addFieldExistsCriterion(query, field));
    }
}
