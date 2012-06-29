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


import org.hibernate.HibernateException;
import org.iternine.jeppetto.dao.AccessControlContext;
import org.iternine.jeppetto.dao.AccessControlException;
import org.iternine.jeppetto.dao.AccessType;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.annotation.AccessControl;
import org.iternine.jeppetto.dao.annotation.Accessor;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AccessControlHelper {

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

    // TODO: check if object already exists -- if yes, modify accessType
    void createEntry(Class<?> objectType, Serializable id, String accessId, AccessType accessType) {
        if (accessType == AccessType.None) {
            return;
        }

        Session session = sessionFactory.getCurrentSession();
        AccessControlEntry accessControlEntry = new AccessControlEntry();

        accessControlEntry.setObjectType(objectType.getSimpleName());
        accessControlEntry.setObjectId(id.toString());
        accessControlEntry.setAccessibleBy(accessId);
        accessControlEntry.setAccessType(accessType.shortName());

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


    Map<String, AccessType> getEntries(Class<?> objectType, Serializable id) {
        Session session = sessionFactory.getCurrentSession();
        Criteria criteria = session.createCriteria(AccessControlEntry.class);

        criteria.add(Restrictions.eq("objectType", objectType.getSimpleName()));
        criteria.add(Restrictions.eq("objectId", id.toString()));

        //noinspection unchecked
        List<AccessControlEntry> accessControlEntries = (List<AccessControlEntry>) criteria.list();

        if (accessControlEntries == null || accessControlEntries.size() == 0) {
            return Collections.emptyMap();
        } else if (accessControlEntries.size() == 1) {
            AccessControlEntry accessControlEntry = accessControlEntries.iterator().next();

            return Collections.singletonMap(accessControlEntry.getAccessibleBy(),
                                            AccessType.getAccessTypeFromShortName(accessControlEntry.getAccessType()));
        } else {
            Map<String, AccessType> result = new HashMap<String, AccessType>();

            for (AccessControlEntry accessControlEntry : accessControlEntries) {
                result.put(accessControlEntry.getAccessibleBy(),
                           AccessType.getAccessTypeFromShortName(accessControlEntry.getAccessType()));
            }

            return result;
        }
    }

    
    void validateContextAllowsWrite(Class<?> objectType, Serializable id, AccessControlContext accessControlContext,
                                    boolean checkIfReadable) {
        if (annotationAllowsAccess(objectType, accessControlContext, AccessType.ReadWrite)
            || accessControlEntryAllowsAccess(objectType, id, accessControlContext.getAccessId(), AccessType.ReadWrite)) {
            return;
        }

        if (checkIfReadable && !accessControlEntryAllowsAccess(objectType, id, accessControlContext.getAccessId(), AccessType.Read)) {
            throw new NoSuchItemException(objectType.getSimpleName(), id.toString());
        }

        throw new AccessControlException("Can't access object [" + id + "] for ReadWrite with " + accessControlContext);
    }


    boolean annotationAllowsAccess(Class<?> objectType, AccessControlContext accessControlContext, AccessType accessType) {
        if (accessType == null) {
            return false;
        }

        AccessControl accessControl;
        if ((accessControl = getAccessControlAnnotation(objectType)) == null) {
            return false;
        }

        Set<String> roles = accessControlContext.getRoles();

        for (Accessor accessor : accessControl.accessors()) {
            if (accessor.access().allows(accessType)
                && (accessor.type() == Accessor.Type.Anyone
                    || (accessor.type() == Accessor.Type.Role && roles != null && roles.contains(accessor.typeValue())))) {
                return true;
            }
        }

        return false;
    }


    boolean accessControlEntryAllowsAccess(Class<?> objectType, Serializable id, String accessId, AccessType accessType) {
        if (accessType == AccessType.None) {
            return false;
        }

        Session session = null;

        try {
            session = sessionFactory.openSession();
            Criteria criteria = session.createCriteria(AccessControlEntry.class);

            criteria.add(Restrictions.eq("objectType", objectType.getSimpleName()));
            criteria.add(Restrictions.eq("objectId", id.toString()));
            criteria.add(Restrictions.eq("accessibleBy", accessId));

            if (accessType == AccessType.Read) {
                criteria.add(Restrictions.in("accessType", Arrays.asList(AccessType.Read.shortName(),
                                                                         AccessType.ReadWrite.shortName())));
            } else {
                criteria.add(Restrictions.eq("accessType", AccessType.ReadWrite.shortName()));
            }

            return criteria.uniqueResult() != null;
        } finally {
            if (session != null) {
                try { session.close(); } catch (HibernateException ignore) { }
            }
        }
    }


    AccessControl getAccessControlAnnotation(Class<?> objectType) {
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
