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

package org.iternine.jeppetto.dao;


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


    //-------------------------------------------------------------
    // Methods - Object
    //-------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Condition condition = (Condition) o;

        return !(constraint != null ? !constraint.equals(condition.constraint) : condition.constraint != null)
               && !(field != null ? !field.equals(condition.field) : condition.field != null);

    }


    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;

        result = 31 * result + (constraint != null ? constraint.hashCode() : 0);

        return result;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        sb.append("Condition");
        sb.append("{ field='").append(field).append('\'');
        sb.append(", constraint=").append(constraint);
        sb.append(" }");

        return sb.toString();
    }
}
