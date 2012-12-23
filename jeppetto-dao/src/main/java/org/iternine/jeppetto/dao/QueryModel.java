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


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class QueryModel {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private List<Condition> conditions;
    private Map<String, List<Condition>> associationConditions = new LinkedHashMap<String, List<Condition>>();
    private List<Sort> sorts;
    private Projection projection;
    private int maxResults = -1;
    private int firstResult = -1;
    private AccessControlContext accessControlContext;


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public List<Condition> getConditions() {
        return conditions;
    }


    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }


    public void addCondition(Condition condition) {
        if (conditions == null) {
            conditions = new ArrayList<Condition>();
        }

        conditions.add(condition);
    }


    public Map<String, List<Condition>> getAssociationConditions() {
        return associationConditions;
    }


    public void setAssociationConditions(Map<String, List<Condition>> associationConditions) {
        this.associationConditions = associationConditions;
    }


    public void addAssociationCondition(String associationField, Condition condition) {
        List<Condition> conditions = associationConditions.get(associationField);

        if (conditions == null) {
            conditions = new ArrayList<Condition>();
            associationConditions.put(associationField, conditions);
        }

        conditions.add(condition);
    }


    public List<Sort> getSorts() {
        return sorts;
    }


    public void setSorts(List<Sort> sorts) {
        this.sorts = sorts;
    }


    public void addSort(SortDirection sortDirection, String sortField) {
        if (sorts == null) {
            sorts = new ArrayList<Sort>(1);  // Usually only 1 sort value
        }

        sorts.add(new Sort(sortField, sortDirection));
    }


    public Projection getProjection() {
        return projection;
    }


    public void setProjection(Projection projection) {
        this.projection = projection;
    }


    public int getMaxResults() {
        return maxResults;
    }


    public void setMaxResults(int maxResults) {
        this.maxResults = maxResults;
    }


    public int getFirstResult() {
        return firstResult;
    }


    public void setFirstResult(int firstResult) {
        this.firstResult = firstResult;
    }


    public AccessControlContext getAccessControlContext() {
        return accessControlContext;
    }


    public void setAccessControlContext(AccessControlContext accessControlContext) {
        this.accessControlContext = accessControlContext;
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

        QueryModel that = (QueryModel) o;

        return firstResult == that.firstResult
               && maxResults == that.maxResults
               && !(accessControlContext != null ? !accessControlContext.equals(that.accessControlContext) : that.accessControlContext != null)
               && !(associationConditions != null ? !associationConditions.equals(that.associationConditions) : that.associationConditions != null)
               && !(conditions != null ? !conditions.equals(that.conditions) : that.conditions != null)
               && !(projection != null ? !projection.equals(that.projection) : that.projection != null)
               && !(sorts != null ? !sorts.equals(that.sorts) : that.sorts != null);
    }


    @Override
    public int hashCode() {
        int result = conditions != null ? conditions.hashCode() : 0;

        result = 31 * result + (associationConditions != null ? associationConditions.hashCode() : 0);
        result = 31 * result + (sorts != null ? sorts.hashCode() : 0);
        result = 31 * result + (projection != null ? projection.hashCode() : 0);
        result = 31 * result + maxResults;
        result = 31 * result + firstResult;
        result = 31 * result + (accessControlContext != null ? accessControlContext.hashCode() : 0);

        return result;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        sb.append("QueryModel {");
        sb.append("\n  conditions=").append(conditions);
        sb.append("\n  associationConditions=").append(associationConditions);
        sb.append("\n  sorts=").append(sorts);
        sb.append("\n  projection=").append(projection);
        sb.append("\n  maxResults=").append(maxResults);
        sb.append("\n  firstResult=").append(firstResult);
        sb.append("\n  accessControlContext=").append(accessControlContext);
        sb.append("\n}");

        return sb.toString();
    }
}
