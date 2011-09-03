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

package org.jeppetto.dao.hibernate;


import org.jeppetto.dao.annotation.AccessControl;
import org.jeppetto.dao.annotation.AccessControlRule;
import org.jeppetto.dao.annotation.AccessControlType;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class AccessControlEntryHelper {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private SessionFactory sessionFactory;


    //-------------------------------------------------------------
    // Methods - IoC
    //-------------------------------------------------------------

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }


    //-------------------------------------------------------------
    // Methods - Package
    //-------------------------------------------------------------

    void createEntry(Class<?> objectType, Serializable id, String accessId) {
        Session session = sessionFactory.getCurrentSession();
        AccessControlEntry accessControlEntry = new AccessControlEntry();

        accessControlEntry.setObjectType(objectType.getSimpleName());
        accessControlEntry.setObjectId(id.toString());
        accessControlEntry.setAccessibleBy(accessId);

        session.save(accessControlEntry);
    }


    void deleteEntry(Class<?> objectType, Serializable id, String accessId) {
        Session session = sessionFactory.getCurrentSession();
        Criteria criteria = session.createCriteria(AccessControlEntry.class);

        criteria.add(Restrictions.eq("objectType", objectType.getSimpleName()));
        criteria.add(Restrictions.eq("objectId", id.toString()));
        criteria.add(Restrictions.eq("accessibleBy", accessId));

        for (Object o : criteria.list()) {
            session.delete(o);
        }
    }


    void deleteAllEntries(Class<?> objectType, Serializable id) {
        Session session = sessionFactory.getCurrentSession();
        Criteria criteria = session.createCriteria(AccessControlEntry.class);

        criteria.add(Restrictions.eq("objectType", objectType.getSimpleName()));
        criteria.add(Restrictions.eq("objectId", id.toString()));

        for (Object o : criteria.list()) {
            session.delete(o);
        }
    }


    List<String> getEntries(Class<?> objectType, Serializable id) {
        List<String> result = new ArrayList<String>();
        Session session = sessionFactory.getCurrentSession();

        Criteria criteria = session.createCriteria(AccessControlEntry.class);

        criteria.add(Restrictions.eq("objectType", objectType.getSimpleName()));
        criteria.add(Restrictions.eq("objectId", id.toString()));

        //noinspection unchecked
        for (AccessControlEntry accessControlEntry : (List<AccessControlEntry>) criteria.list()) {
            result.add(accessControlEntry.getAccessibleBy());
        }

        return result;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private boolean roleAllowsAccess(Class<?> objectType, String role) {
        AccessControl accessControl;

        if (role == null || role.isEmpty()) {
            return false;
        }

        if ((accessControl = getAccessControlAnnotation(objectType)) == null) {
            return false;
        }

        for (AccessControlRule accessControlRule : accessControl.rules()) {
            if (accessControlRule.type() == AccessControlType.Role && accessControlRule.value().equals(role)) {
                return true;
            }
        }

        return false;
    }


    private AccessControl getAccessControlAnnotation(Class<?> objectType) {
        while (objectType != null) {
            // noinspection unchecked
            AccessControl accessControl = objectType.getAnnotation(AccessControl.class);

            if (accessControl != null) {
                return accessControl;
            }

            objectType = objectType.getSuperclass();
        }

        return null;
    }
}
