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


import java.io.Serializable;


/**
 * A GenericDAO is the root interface for DAOs that fit within the Jeppetto framework.  This
 * defines base functionality such as finding a specific item by its identifier and saving an
 * to the underlying data store.
 *
 * @param <T> Persistent Class
 * @param <PK> Primary key of T
 */
public interface GenericDAO<T, PK extends Serializable> {

    /**
     * Find an object T with the specified primary key.
     *
     * @param primaryKey of the desired object.
     *
     * @return Object with the specified primary key
     *
     * @throws NoSuchItemException if the object identified by the primary key is not found
     */
    T findById(PK primaryKey)
            throws NoSuchItemException;


    /**
     * Find all objects of type T.
     *
     * @return Iterable of T
     */
    Iterable<T> findAll();


    /**
     * Call save to insert a new object into the persistent store or update a preexisting object that has
     * been modified.
     *
     * @param object to save.
     */
    void save(T object);


    /**
     * Delete the specified object from the persistent store.
     *
     * @param object to delete.
     */
    void delete(T object);


    /**
     * Delete an object from the persistent store based on the primary key.
     *
     * @param primaryKey of the object to delete.
     */
    void delete(PK primaryKey);


    /**
     * If the implementation supports lazy writes, manually flush changes to the external database
     */
    void flush();
}
