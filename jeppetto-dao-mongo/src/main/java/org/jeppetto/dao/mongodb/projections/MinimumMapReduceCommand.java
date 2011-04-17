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

package org.jeppetto.dao.mongodb.projections;


import com.mongodb.DBObject;


/**
 * Map reduce job that computes a field's lowest value.
 */
class MinimumMapReduceCommand
        extends MapReduceCommand {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String field;


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    public MinimumMapReduceCommand(DBObject query, String field) {
        super(query);

        this.field = field;
    }


    //-------------------------------------------------------------
    // Override - MapReduceCommand
    //-------------------------------------------------------------

    @Override
    protected Object defaultValue() {
        return null;
    }


    @Override
    protected String createMapFunction() {
        return String.format("function() { emit( 1, this.%1$s ); }", field);
    }


    @Override
    protected String createReduceFunction() {
        return "function(k,v) { " +
                "var min; " +
                "for(var i in v) { " +
                "    if (i == 0 || v[i] < min) { " +
                "        min = v[i]; " +
                "    } " +
                "} " +
                "return min; " +
                "}";
    }


    @Override
    protected Object transformToValue(DBObject dbObject) {
        return dbObject.get("value");
    }
}
