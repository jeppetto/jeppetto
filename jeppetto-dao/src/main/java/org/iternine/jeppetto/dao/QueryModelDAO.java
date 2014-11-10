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


import java.util.Iterator;


/**
 * The QueryModelDAO interface defines an extension to the GenericDAO
 * functionality that offers significantly more richness around the
 * representation of queries to retrieve a specific object or a list of zero
 * or more items that match the given QueryModel.
 *
 * @param <T> Persistent Class
 * @param <ID> ID type for the persistent class.
 */
public interface QueryModelDAO<T, ID> extends GenericDAO<T, ID> {

    /**
     * Find an object T that satisfies the QueryModel.
     *
     * @param queryModel that contains criteria to match
     *
     * @return Object that satisfies the query model
     *
     * @throws NoSuchItemException if the object identified by the queryModel is not found
     * @throws TooManyItemsException if more than one object identified by the queryModel is found
     * @throws JeppettoException if any other failure occurs
     */
    T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException, TooManyItemsException, JeppettoException;


    /**
     * Find objects of type T that correspond to the QueryModel.
     *
     * @param queryModel that contains criteria that will be true of the results
     *
     * @return Iterable of T
     *
     * @throws JeppettoException if any underlying failure occurs
     */
    Iterable<T> findUsingQueryModel(QueryModel queryModel)
            throws JeppettoException;


    /**
     * Use the QueryModel to narrow a set of results, then perform the
     * specified projection.
     *
     * @param queryModel that contains criteria that will be used
     *
     * @return result of the projection - exact type depends on the projection
     *         specified
     *
     * @throws JeppettoException if any underlying failure occurs
     */
    Object projectUsingQueryModel(QueryModel queryModel)
            throws JeppettoException;


    /**
     * Delete objects of type T that correspond to the QueryModel.
     *
     * @param queryModel that contains criteria of items to delete
     *
     * @throws JeppettoException if any underlying failure occurs
     */
    void deleteUsingQueryModel(QueryModel queryModel)
            throws JeppettoException;


    /**
     *
     */
    ReferenceSet<T> referenceUsingQueryModel(QueryModel queryModel)
            throws JeppettoException;


    /**
     * Construct a Condition object given the passed in arguments.  This is
     * used to build up the resulting QueryModel that will ultimately be
     * passed back to the one of the find/project methods above.
     *
     * @param conditionField the field upon which the Condition should be made
     * @param conditionType the type of Condition object to construct
     * @param argsIterator an Iterator that may contain values used during the
     *                     construction of the Condition
     *
     * @return corresponding Condition object
     */
    Condition buildCondition(String conditionField,
                             ConditionType conditionType,
                             Iterator argsIterator);


    /**
     * Construct a Projection object given the passed in arguments.  This is
     * used to build up the resulting QueryModel that will ultimately be
     * passed back to the projectUsingQueryModel() method.
     *
     * @param projectionField the field upon which the Projection should be made
     * @param projectionType the type of Projection object to construct
     * @param argsIterator an Iterator that may contain values used during the
     *                     construction of the Projection
     *
     * @return corresponding Projection object
     */
    Projection buildProjection(String projectionField,
                               ProjectionType projectionType,
                               Iterator argsIterator);
}
