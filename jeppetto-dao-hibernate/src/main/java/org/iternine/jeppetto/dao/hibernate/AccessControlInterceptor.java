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

package org.iternine.jeppetto.dao.hibernate;


import org.iternine.jeppetto.dao.AccessControlContext;
import org.iternine.jeppetto.dao.AccessControlContextProvider;

import org.hibernate.EmptyInterceptor;
import org.hibernate.type.Type;
import org.iternine.jeppetto.dao.AccessControlException;
import org.iternine.jeppetto.dao.AccessType;
import org.iternine.jeppetto.dao.annotation.AccessControl;
import org.iternine.jeppetto.dao.annotation.Creator;

import java.io.Serializable;


public class AccessControlInterceptor extends EmptyInterceptor {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private AccessControlHelper accessControlHelper;
    private AccessControlContextProvider accessControlContextProvider;


    //-------------------------------------------------------------
    // Implementation - Interceptor
    //-------------------------------------------------------------

    @Override
    public boolean onFlushDirty(Object entity, Serializable id, Object[] currentState, Object[] previousState,
                                String[] propertyNames, Type[] types) {
        if (entity.getClass().isAssignableFrom(AccessControlEntry.class)) {
            return false;
        }

        AccessControlContext accessControlContext = AccessControlContextOverride.exists() ? AccessControlContextOverride.get()
                                                                                          : accessControlContextProvider.getCurrent();

        accessControlHelper.validateContextAllowsWrite(entity.getClass(), id, accessControlContext, false);

        return false;
    }


    @Override
    public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
        // TODO: Determine if this is a new or existing object and validate write access
        if (entity.getClass().isAssignableFrom(AccessControlEntry.class)) {
            return false;
        }

        AccessControlContext accessControlContext = AccessControlContextOverride.exists() ? AccessControlContextOverride.get()
                                                                                          : accessControlContextProvider.getCurrent();

        AccessControl accessControl = accessControlHelper.getAccessControlAnnotation(entity.getClass());
        if (accessControl != null) {
            for (Creator creator : accessControl.creators()) {
                switch (creator.type()) {
                case Identified:
                    if (accessControlContext.getAccessId() != null) {
                        accessControlHelper.createEntry(entity.getClass(), id, accessControlContext.getAccessId(), creator.grantedAccess());

                        return false;
                    }

                    break;

                case Role:
                    if (accessControlContext.getRoles() != null && accessControlContext.getRoles().contains(creator.typeValue())) {
                        accessControlHelper.createEntry(entity.getClass(), id, accessControlContext.getAccessId(), creator.grantedAccess());

                        return false;
                    }

                    break;

                case Anonymous:
                    return false;
                }
            }

            throw new AccessControlException("Unable to create " + entity.getClass().getSimpleName()
                                             + " with " + accessControlContext
                                             + ".  Check object's @AccessControl annotation.");
        } else {
            // When no annotation is present, any user can create the object.  If user is unknown, no explicit grants.
            // Otherwise, ReadWrite access is given to the caller.
            if (accessControlContext.getAccessId() != null) {
                accessControlHelper.createEntry(entity.getClass(), id, accessControlContext.getAccessId(), AccessType.ReadWrite);
            }
        }

        return false;
    }


    @Override
    public void onDelete(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
        // TODO: Determine if this is a new or existing object and validate write access
        if (!entity.getClass().isAssignableFrom(AccessControlEntry.class)) {
            accessControlHelper.deleteAllEntries(entity.getClass(), id);
        }
    }


    //-------------------------------------------------------------
    // Methods - IoC
    //-------------------------------------------------------------

    public void setAccessControlHelper(AccessControlHelper accessControlHelper) {
        this.accessControlHelper = accessControlHelper;
    }


    public void setAccessControlContextProvider(AccessControlContextProvider accessControlContextProvider) {
        this.accessControlContextProvider = accessControlContextProvider;
    }
}
