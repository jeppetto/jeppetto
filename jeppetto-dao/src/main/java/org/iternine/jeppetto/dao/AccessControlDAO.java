/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
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


import java.util.Map;


/**
 * @param <T> Persistent Class
 * @param <ID> ID type of the persistent class.
 */
public interface AccessControlDAO<T, ID> extends GenericDAO<T, ID> {

    void save(T object, AccessControlContext accessControlContext)
            throws OptimisticLockException, AccessControlException, JeppettoException;


    void grantAccess(ID id, String accessId, AccessType accessType)
            throws NoSuchItemException, AccessControlException;


    void grantAccess(ID id, String accessId, AccessType accessType, AccessControlContext accessControlContext)
            throws NoSuchItemException, AccessControlException;


    void revokeAccess(ID id, String accessId)
            throws NoSuchItemException, AccessControlException;


    void revokeAccess(ID id, String accessId, AccessControlContext accessControlContext)
            throws NoSuchItemException, AccessControlException;


    Map<String, AccessType> getGrantedAccesses(ID id)
            throws NoSuchItemException, AccessControlException;


    Map<String, AccessType> getGrantedAccesses(ID id, AccessControlContext accessControlContext)
            throws NoSuchItemException, AccessControlException;


    AccessControlContextProvider getAccessControlContextProvider();
}
