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

package org.iternine.jeppetto.dao.hibernate;


import org.iternine.jeppetto.dao.AccessControlContextProvider;

import org.hibernate.EmptyInterceptor;
import org.hibernate.type.Type;

import java.io.Serializable;


public class AccessControlInterceptor extends EmptyInterceptor {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private AccessControlEntryHelper accessControlEntryHelper;
    private AccessControlContextProvider accessControlContextProvider;


    //-------------------------------------------------------------
    // Implementation - Interceptor
    //-------------------------------------------------------------

    @Override
    public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
        if (!entity.getClass().isAssignableFrom(AccessControlEntry.class)
            && accessControlContextProvider.getCurrent().getAccessId() != null) {
            accessControlEntryHelper.createEntry(entity.getClass(), id, accessControlContextProvider.getCurrent().getAccessId());
        }

        return false;
    }


    @Override
    public void onDelete(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
        if (!entity.getClass().isAssignableFrom(AccessControlEntry.class)) {
            accessControlEntryHelper.deleteAllEntries(entity.getClass(), id);
        }
    }


    //-------------------------------------------------------------
    // Methods - IoC
    //-------------------------------------------------------------

    public void setAccessControlHelper(AccessControlEntryHelper accessControlEntryHelper) {
        this.accessControlEntryHelper = accessControlEntryHelper;
    }


    public void setAccessControlContextProvider(AccessControlContextProvider accessControlContextProvider) {
        this.accessControlContextProvider = accessControlContextProvider;
    }
}
