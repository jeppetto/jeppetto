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

package org.iternine.jeppetto.dao;


public class Sort {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String field;
    private SortDirection sortDirection;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public Sort(String field, SortDirection sortDirection) {
        this.field = field;
        this.sortDirection = sortDirection;
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


    public SortDirection getSortDirection() {
        return sortDirection;
    }


    public void setSortDirection(SortDirection sortDirection) {
        this.sortDirection = sortDirection;
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

        Sort sort = (Sort) o;

        return !(field != null ? !field.equals(sort.field) : sort.field != null)
               && sortDirection == sort.sortDirection;

    }


    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;

        result = 31 * result + (sortDirection != null ? sortDirection.hashCode() : 0);

        return result;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        sb.append("Sort");
        sb.append("{ field='").append(field).append('\'');
        sb.append(", sortDirection=").append(sortDirection);
        sb.append(" }");

        return sb.toString();
    }
}
