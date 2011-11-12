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


/**
 * A GenericDAO is the root interface for DAOs that fit within the Jeppetto framework.  This
 * defines base functionality such as finding a specific item by its identifier and saving an
 * to the underlying data store.
 *
 * @param <T> Persistent Class
 * @param <ID> ID type for the persistent class.
 */
public interface GenericDAO<T, ID> {

    /**
     * Find an object T with the specified id.
     *
     * @param id of the desired object.
     *
     * @return Object with the specified id
     *
     * @throws NoSuchItemException if the object identified by the id is not found
     * @throws JeppettoException if any other failure occurs
     */
    T findById(ID id)
            throws NoSuchItemException, JeppettoException;


    /**
     * Find all objects of type T.
     *
     * @return Iterable of T
     *
     * @throws JeppettoException if any underlying failure occurs
     */
    Iterable<T> findAll()
            throws JeppettoException;


    /**
     * Call save to insert a new object into the persistent store or update a preexisting object that has
     * been modified.
     *
     * @param object to save.
     *
     * @throws OptimisticLockException if optimistic locking is enabled and a save is attempted on an already
     *                                 modified object.
     * @throws JeppettoException if any other failure occurs
     */
    void save(T object)
            throws OptimisticLockException, JeppettoException;


    /**
     * Delete the specified object from the persistent store.
     *
     * @param object to delete.
     *
     * @throws JeppettoException if any underlying failure occurs
     */
    void delete(T object)
            throws JeppettoException;


    /**
     * Delete an object from the persistent store based on the id.
     *
     * @param id of the object to delete.
     *
     * @throws JeppettoException if any underlying failure occurs
     */
    void deleteById(ID id)
            throws JeppettoException;


    /**
     * If the implementation supports lazy writes, manually flush changes to the external database
     *
     * @throws JeppettoException if any underlying failure occurs
     */
    void flush()
            throws JeppettoException;
}
