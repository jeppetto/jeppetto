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

package org.jeppetto.dao;


public class Condition {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String field;
    private Object constraint;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public Condition() {
    }


    public Condition(String field, Object constraint) {
        this.field = field;
        this.constraint = constraint;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getField() {
        return field;
    }


    public void setField(String field) {
        this.field = field;
    }


    public Object getConstraint() {
        return constraint;
    }


    public void setConstraint(Object constraint) {
        this.constraint = constraint;
    }
}
