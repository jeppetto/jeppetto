/*
 * Copyright (c) 2012 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.test.accesscontrol;


import org.iternine.jeppetto.dao.AccessControlContext;
import org.iternine.jeppetto.dao.AccessControlException;
import org.iternine.jeppetto.dao.AccessType;
import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.SimpleAccessControlContext;
import org.iternine.jeppetto.test.SettableAccessControlContextProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.relation.Role;
import java.util.Collections;
import java.util.Map;


public abstract class AccessControlTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private SettableAccessControlContextProvider accessControlContextProvider;

    private static SimpleAccessControlContext identifiedUser;
    private static SimpleAccessControlContext userWithCreatorsRole;
    private static SimpleAccessControlContext userWithAccessorsRole;
    private static SimpleAccessControlContext administrator;
    private static SimpleAccessControlContext anotherUser;
    private static SimpleAccessControlContext anonymousUser;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    static {
        identifiedUser = new SimpleAccessControlContext();
        identifiedUser.setAccessId("001");
        // no role specified

        userWithCreatorsRole = new SimpleAccessControlContext();
        userWithCreatorsRole.setAccessId("002");
        userWithCreatorsRole.setRoles(Collections.singleton("Creators"));

        userWithAccessorsRole = new SimpleAccessControlContext();
        userWithAccessorsRole.setAccessId("002");
        userWithAccessorsRole.setRoles(Collections.singleton("Accessors"));

        administrator = new SimpleAccessControlContext();
        administrator.setAccessId("003");
        administrator.setRoles(Collections.singleton("Administrator"));

        anotherUser = new SimpleAccessControlContext();
        anotherUser.setAccessId("004");
        anotherUser.setRoles(Collections.singleton("Administrator"));

        anonymousUser = new SimpleAccessControlContext();
    }


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract DefaultAccessObjectDAO getDefaultAccessObjectDAO();
    protected abstract IdentifiedCreatableObjectDAO getIdentifiedCreatableObjectDAO();
    protected abstract RoleCreatableObjectDAO getRoleCreatableObjectDAO();

    protected abstract void reset();


    //-------------------------------------------------------------
    // Methods - Test Lifecycle
    //-------------------------------------------------------------

    @Before
    public void before() {
        accessControlContextProvider = (SettableAccessControlContextProvider) getDefaultAccessObjectDAO().getAccessControlContextProvider();
    }


    @After
    public void after() {
        reset();

        accessControlContextProvider = null;
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void createObjectsWithUserWithCreatorsRole() {
        saveObjectWithContext(userWithCreatorsRole, new DefaultAccessObject(), getDefaultAccessObjectDAO());
        saveObjectWithContext(userWithCreatorsRole, new IdentifiedCreatableObject(), getIdentifiedCreatableObjectDAO());
        saveObjectWithContext(userWithCreatorsRole, new RoleCreatableObject(), getRoleCreatableObjectDAO());
    }


    @Test
    public void createObjectsWithIdentifiedUser() {
        saveObjectWithContext(identifiedUser, new DefaultAccessObject(), getDefaultAccessObjectDAO());
        saveObjectWithContext(identifiedUser, new IdentifiedCreatableObject(), getIdentifiedCreatableObjectDAO());

        try {
            saveObjectWithContext(identifiedUser, new RoleCreatableObject(), getRoleCreatableObjectDAO());

            throw new RuntimeException("Expected AccessControlException");
        } catch (AccessControlException ignore) {
        }
    }


    @Test
    public void createObjectsWithAnonymousUser() {
        saveObjectWithContext(anonymousUser, new DefaultAccessObject(), getDefaultAccessObjectDAO());

        try {
            saveObjectWithContext(anonymousUser, new IdentifiedCreatableObject(), getIdentifiedCreatableObjectDAO());

            throw new RuntimeException("Expected AccessControlException");
        } catch (AccessControlException ignore) {
        }

        try {
            saveObjectWithContext(anonymousUser, new RoleCreatableObject(), getRoleCreatableObjectDAO());

            throw new RuntimeException("Expected AccessControlException");
        } catch (AccessControlException ignore) {
        }
    }


    @Test
    public void unauthorizedAccessAttempts()
            throws AccessControlException, NoSuchItemException {
        String id = saveObjectWithContext(identifiedUser, new DefaultAccessObject(), getDefaultAccessObjectDAO());

        // Make sure this user can get the object
        getObjectWithContext(identifiedUser, id, getDefaultAccessObjectDAO());

        // No on else should be able to get it

        try {
            getObjectWithContext(userWithCreatorsRole, id, getDefaultAccessObjectDAO());

            throw new RuntimeException("Expected NoSuchItemException");
        } catch (NoSuchItemException ignore) {
        }

        try {
            getObjectWithContext(anonymousUser, id, getDefaultAccessObjectDAO());

            throw new RuntimeException("Expected NoSuchItemException");
        } catch (NoSuchItemException ignore) {
        }

        // Revoke the creator's accessId (set appropriate context so able to access it)
        accessControlContextProvider.setCurrent(identifiedUser);
        getDefaultAccessObjectDAO().revokeAccess(id, identifiedUser.getAccessId());

        try {
            // Now even this person can't access it.
            getObjectWithContext(identifiedUser, id, getDefaultAccessObjectDAO());

            throw new RuntimeException("Expected NoSuchItemException");
        } catch (NoSuchItemException ignore) {
        }

        try {
            getObjectWithContext(anonymousUser, id, getDefaultAccessObjectDAO());

            throw new RuntimeException("Expected NoSuchItemException");
        } catch (NoSuchItemException ignore) {
        }
    }


    @Test
    public void grantedAccessAttempt()
            throws NoSuchItemException {
        String id = saveObjectWithContext(identifiedUser, new DefaultAccessObject(), getDefaultAccessObjectDAO());

        accessControlContextProvider.setCurrent(identifiedUser);
        getDefaultAccessObjectDAO().grantAccess(id, userWithCreatorsRole.getAccessId(), AccessType.Read);

        getObjectWithContext(userWithCreatorsRole, id, getDefaultAccessObjectDAO());

        try {
            // Reader can't grant access
            accessControlContextProvider.setCurrent(userWithCreatorsRole);

            getDefaultAccessObjectDAO().grantAccess(id, anotherUser.getAccessId(), AccessType.Read);

            throw new RuntimeException("Expected AccessControlException");
        } catch (AccessControlException ignore) {
        }

        try {
            // User can't upgrade self to ReadWrite
            accessControlContextProvider.setCurrent(userWithCreatorsRole);
            getDefaultAccessObjectDAO().grantAccess(id, userWithCreatorsRole.getAccessId(), AccessType.ReadWrite);

            throw new RuntimeException("Expected AccessControlException");
        } catch (AccessControlException ignore) {
        }

        // ReadWriter can upgrade user
        accessControlContextProvider.setCurrent(identifiedUser);
        getDefaultAccessObjectDAO().grantAccess(id, userWithCreatorsRole.getAccessId(), AccessType.ReadWrite);

        // Upgraded user can grant access
        accessControlContextProvider.setCurrent(userWithCreatorsRole);
        getDefaultAccessObjectDAO().grantAccess(id, anotherUser.getAccessId(), AccessType.Read);

        getObjectWithContext(anotherUser, id, getDefaultAccessObjectDAO());

        // Check AccessTypes
        accessControlContextProvider.setCurrent(identifiedUser);
        Map<String, AccessType> accessTypeMap = getDefaultAccessObjectDAO().getGrantedAccesses(id);
        Assert.assertEquals(3, accessTypeMap.size());
        Assert.assertEquals(AccessType.ReadWrite, accessTypeMap.get(identifiedUser.getAccessId()));
        Assert.assertEquals(AccessType.ReadWrite, accessTypeMap.get(userWithCreatorsRole.getAccessId()));
        Assert.assertEquals(AccessType.Read, accessTypeMap.get(anotherUser.getAccessId()));
    }


    @Test
    public void updateObjectWithCreatorContext() {
        String id = saveObjectWithContext(identifiedUser, new DefaultAccessObject(), getDefaultAccessObjectDAO());

        DefaultAccessObject defaultAccessObject = getObjectWithContext(identifiedUser, id, getDefaultAccessObjectDAO());

        defaultAccessObject.setIntValue(5);

        saveObjectWithContext(identifiedUser, defaultAccessObject, getDefaultAccessObjectDAO());
    }


    @Test
    public void updateObjectWithReadWriteContext() {
        String id = saveObjectWithContext(identifiedUser, new IdentifiedCreatableObject(), getIdentifiedCreatableObjectDAO());

        IdentifiedCreatableObject identifiedCreatableObject = getObjectWithContext(administrator, id, getIdentifiedCreatableObjectDAO());

        identifiedCreatableObject.setIntValue(5);

        saveObjectWithContext(identifiedUser, identifiedCreatableObject, getIdentifiedCreatableObjectDAO());
    }


    @Test(expected = AccessControlException.class)
    public void updateObjectWithReadContext() {
        String id = saveObjectWithContext(identifiedUser, new DefaultAccessObject(), getDefaultAccessObjectDAO());

        accessControlContextProvider.setCurrent(identifiedUser);
        getDefaultAccessObjectDAO().grantAccess(id, userWithCreatorsRole.getAccessId(), AccessType.Read);

        DefaultAccessObject defaultAccessObject = getObjectWithContext(userWithCreatorsRole, id, getDefaultAccessObjectDAO());

        defaultAccessObject.setIntValue(5);

        saveObjectWithContext(userWithCreatorsRole, defaultAccessObject, getDefaultAccessObjectDAO());
    }


    @Test
    public void allowedRoleAccessAttempt()
            throws NoSuchItemException {
        String id = saveObjectWithContext(identifiedUser, new IdentifiedCreatableObject(), getIdentifiedCreatableObjectDAO());

        getObjectWithContext(administrator, id, getIdentifiedCreatableObjectDAO());
    }


    @Test
    public void createAndGetOwnObjects() {
        for (int i = 0; i < 10; i++) {
            saveObjectWithContext(identifiedUser, new DefaultAccessObject(), getDefaultAccessObjectDAO());
        }

        for (int i = 0; i < 5; i++) {
            saveObjectWithContext(userWithCreatorsRole, new DefaultAccessObject(), getDefaultAccessObjectDAO());
        }

        accessControlContextProvider.setCurrent(identifiedUser);

        Iterable<DefaultAccessObject> identifiedUserObjects = getDefaultAccessObjectDAO().findAll();

        String randomId = null;
        int count = 0;

        for (DefaultAccessObject defaultAccessObject : identifiedUserObjects) {
            if (randomId == null) {
                randomId = defaultAccessObject.getId();
            }

            count++;
        }

        Assert.assertEquals(10, count);

        accessControlContextProvider.setCurrent(userWithCreatorsRole);

        Iterable<DefaultAccessObject> userWithCreatorsRoleObjects = getDefaultAccessObjectDAO().findAll();

        int count2 = 0;
        //noinspection UnusedDeclaration
        for (DefaultAccessObject defaultAccessObject : userWithCreatorsRoleObjects) {
            Assert.assertNotSame(randomId, defaultAccessObject.getId());

            count2++;
        }

        Assert.assertEquals(5, count2);
    }


    @Test
    public void creatorWithAccessTypeNoneCantAccessObject() {
        String id = saveObjectWithContext(userWithCreatorsRole, new RoleCreatableObject(), getRoleCreatableObjectDAO());

        try {
            getObjectWithContext(userWithCreatorsRole, id, getRoleCreatableObjectDAO());

            throw new RuntimeException("Creator should be able to access this object (grantedAccess of None prohibits)");
        } catch (NoSuchItemException ignore) {
        }

        getObjectWithContext(userWithAccessorsRole, id, getRoleCreatableObjectDAO());
    }


    @Test
    public void creatorCanAccessObjectWhenUsingAnnotation() {
        String id = saveObjectWithContext(userWithCreatorsRole, new RoleCreatableObject(), getRoleCreatableObjectDAO());

        try {
            getObjectWithContext(userWithCreatorsRole, id, getRoleCreatableObjectDAO());

            throw new RuntimeException("Creator should be able to access this object (grantedAccess of None prohibits)");
        } catch (NoSuchItemException ignore) {
        }

        RoleCreatableObject roleCreatableObject = getRoleCreatableObjectDAO().privilegedFindById(id);

        Assert.assertEquals(id, roleCreatableObject.getId());
    }


//    @Test
//    public void verifyOrderByWorks() {
//        accessControlContextProvider.setCurrent(accessControlContext1);
//
//        for (int i = 0; i < 10; i++) {
//            getAccessControlTestDAO().save(new SimpleObject());
//        }
//
//        List<SimpleObject> orderedItems = getAccessControlTestDAO().findByOrderById();
//
//        Assert.assertEquals(10, orderedItems.size());
//
//        String lastId = null;
//        for (SimpleObject orderedItem : orderedItems) {
//            if (lastId != null) {
//                Assert.assertTrue("lastId is not less than thisId: " + lastId + " !< " + orderedItem.getId(),
//                                  lastId.compareTo(orderedItem.getId()) < 0);
//            }
//
//            lastId = orderedItem.getId();
//        }
//    }
//
//
//    @Test
//    public void associationAccessAttempt()
//            throws NoSuchItemException {
//        accessControlContextProvider.setCurrent(accessControlContext1);
//
//        SimpleObject simpleObject = new SimpleObject();
//
//        RelatedObject relatedObject = new RelatedObject();
//        relatedObject.setRelatedStringValue("foo");
//
//        simpleObject.setRelatedObjectSet(Collections.singleton(relatedObject));
//
//        getAccessControlTestDAO().save(simpleObject);
//
//        simpleObject = new SimpleObject();
//
//        getAccessControlTestDAO().save(simpleObject);
//
//        List<SimpleObject> resultObjects
//                = getAccessControlTestDAO().findByHavingRelatedObjectSetWithRelatedStringValue("foo");
//
//        Assert.assertEquals(1, resultObjects.size());
//    }
//
//
//    @Test
//    public void checkAnnotationQueryWorks() {
//        accessControlContextProvider.setCurrent(accessControlContext1);
//
//        for (int i = 1; i < 10; i++) {
//            getAccessControlTestDAO().save(new SimpleObject(i));
//        }
//
//        accessControlContextProvider.setCurrent(accessControlContext2);
//
//        for (int i = 2; i < 10; i++) {
//            getAccessControlTestDAO().save(new SimpleObject(i));
//        }
//
//        accessControlContextProvider.setCurrent(accessControlContext1);
//
//        Assert.assertEquals(3, getAccessControlTestDAO().getByIntValueLessThan(4).size());
//        Assert.assertEquals(2, getAccessControlTestDAO().getByIntValueLessThanSpecifyingContext(4, accessControlContext2).size());
//        Assert.assertEquals(5, getAccessControlTestDAO().getByIntValueLessThanUsingAdministratorRole(4).size());
//        Assert.assertEquals(0, getAccessControlTestDAO().getByIntValueLessThanUsingBogusRole(4).size());
//    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private String saveObjectWithContext(AccessControlContext context, IdentifiableObject object, GenericDAO dao) {
        accessControlContextProvider.setCurrent(context);

        try {
            dao.save(object);

            return object.getId();
        } finally {
            accessControlContextProvider.setCurrent(null);
        }
    }


    @SuppressWarnings("unchecked")
    private <T extends IdentifiableObject, ID> T getObjectWithContext(AccessControlContext context, ID id, GenericDAO<T, ID> dao)
            throws AccessControlException, NoSuchItemException {
        accessControlContextProvider.setCurrent(context);

        try {
            T resultObject = dao.findById(id);

            Assert.assertEquals(resultObject.getId(), id);

            return resultObject;
        } finally {
            accessControlContextProvider.setCurrent(null);
        }
    }
}
