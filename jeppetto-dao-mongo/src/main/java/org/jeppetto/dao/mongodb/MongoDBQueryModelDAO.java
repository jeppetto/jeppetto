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

package org.jeppetto.dao.mongodb;


import org.jeppetto.dao.AccessControlContextProvider;
import org.jeppetto.dao.AccessControllable;
import org.jeppetto.dao.Condition;
import org.jeppetto.dao.ConditionType;
import org.jeppetto.dao.NoSuchItemException;
import org.jeppetto.dao.Projection;
import org.jeppetto.dao.ProjectionType;
import org.jeppetto.dao.QueryModel;
import org.jeppetto.dao.QueryModelDAO;
import org.jeppetto.dao.Sort;
import org.jeppetto.dao.SortDirection;
import org.jeppetto.dao.annotation.AccessControl;
import org.jeppetto.dao.annotation.AccessControlRule;
import org.jeppetto.dao.annotation.AccessControlType;
import org.jeppetto.dao.mongodb.enhance.DBObjectUtil;
import org.jeppetto.dao.mongodb.enhance.DirtyableDBObject;
import org.jeppetto.dao.mongodb.enhance.EnhancerHelper;
import org.jeppetto.dao.mongodb.enhance.MongoDBCallback;
import org.jeppetto.dao.mongodb.projections.ProjectionCommands;
import org.jeppetto.enhance.Enhancer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCallback;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Provides a QueryModelDAO implementation for Mongo.
 *
 * Provides bi-directional ODM (Object to Document Mapping) as well as the
 * rich query capabilities found in the core org.jeppetto.dao package.
 *
 * This implementation of the QueryModelDAO requires that the PK be a String.
 *
 * Instantiation requires a daoProperties Map<String, Object> that will look
 * for the following keys (and expected values):
 *
 *   "db" -> Instance of a configured com.mongodb.DB        (required)
 *   "uniqueIndexes" -> List<String> of various MongoDB     (optional)
 *                      index values that will be
 *                      ensured to exist and must be
 *                      unique
 *   "nonUniqueIndexes" -> List<String> of various MongoDB  (optional)
 *                         index values that will be
 *                         ensured to exist and need
 *                         not be unique
 *   "optimisticLockEnabled" -> Boolean as to whether       (optional)
 *                              instances of the tracked
 *                              type should have an
 *                              implicit lock version
 *                              field
 *   "saveNulls" -> Boolean as to whether null fields       (optional)
 *                  should be included in saved objects.
 *                  If a field goes from a set value to
 *                  null, it will be cleared.
 *   "writeConcern" -> String, one of the values as         (optional)
 *                     indicated at http://www.mongodb.org/display/DOCS/Replica+Set+Semantics.
 *                     If not specified, the DAO defaults
 *                     to "SAFE".  In either case, it can
 *                     be overridden on a call-by-call
 *                     basis by ...
 *                     TODO: implement... (keep in mind session semantics...)
 *   "showQueries" -> Boolean that indicates if additional  (optional)
 *                    info should be logged regarding the
 *                    various queries that are executed.
 *                    Logging will need need to be enabled
 *                    for the DAO's package
 *
 * @param <T> the type of persistent object this DAO will manage.
 */
// TODO: support createdDate/lastModifiedDate (createdDate from get("_id").getTime()?)
// TODO: Implement determineOptimalDBObject
//          - understand saveNulls
//          - understand field-level deltas
// TODO: become shard-aware
// TODO: support per-call WriteConcerns
// TODO: investigate usage of ClassLoader so new instances are already enhanced
public class MongoDBQueryModelDAO<T>
        implements QueryModelDAO<T, String>, AccessControllable<String> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final String ID_FIELD = "_id";
    private static final String OPTIMISTIC_LOCK_VERSION_FIELD = "__olv";
    private static final String ACCESS_CONTROL_LIST_FIELD = "__acl";
    private static final Map<ConditionType, MongoDBOperator> CONDITION_TYPE_TO_MONGO_OPERATOR = new HashMap<ConditionType, MongoDBOperator>() { {
            put(ConditionType.Equal, MongoDBOperator.Equal);
            put(ConditionType.NotEqual, MongoDBOperator.NotEqual);
            put(ConditionType.GreaterThan, MongoDBOperator.GreaterThan);
            put(ConditionType.GreaterThanEqual, MongoDBOperator.GreaterThanEqual);
            put(ConditionType.LessThan, MongoDBOperator.LessThan);
            put(ConditionType.LessThanEqual, MongoDBOperator.LessThanEqual);
            put(ConditionType.Between, MongoDBOperator.Between);
            put(ConditionType.Within, MongoDBOperator.Within);
            put(ConditionType.NotWithin, MongoDBOperator.NotWithin);
//            put(ConditionType.IsNull, MongoDBOperator.IsNull);
//            put(ConditionType.IsNotNull, MongoDBOperator.IsNotNull);
        }
    };


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private DBCollection dbCollection;
    private Enhancer<T> enhancer;
    private AccessControlContextProvider accessControlContextProvider;
    private Map<String, Set<String>> uniqueIndexes;
    private boolean optimisticLockEnabled;
    private boolean saveNulls;
    private WriteConcern defaultWriteConcern;
    private Logger queryLogger;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected MongoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties) {
        this(entityClass, daoProperties, null);
    }


    @SuppressWarnings( { "unchecked" })
    protected MongoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties,
                                   AccessControlContextProvider accessControlContextProvider) {
        this.dbCollection = ((DB) daoProperties.get("db")).getCollection(entityClass.getSimpleName());
        this.enhancer = EnhancerHelper.getDirtyableDBObjectEnhancer(entityClass);

        dbCollection.setObjectClass(enhancer.getEnhancedClass());

        DBCallback.FACTORY = new DBCallback.Factory() {
            @Override
            public DBCallback create(DBCollection dbCollection) {
                return new MongoDBCallback(dbCollection);
            }
        };

        this.accessControlContextProvider = accessControlContextProvider;
        this.uniqueIndexes = ensureIndexes((List<String>) daoProperties.get("uniqueIndexes"), true);
        ensureIndexes((List<String>) daoProperties.get("nonUniqueIndexes"), false);
        this.optimisticLockEnabled = Boolean.parseBoolean((String) daoProperties.get("optimisticLockEnabled"));
        this.saveNulls = Boolean.parseBoolean((String) daoProperties.get("saveNulls"));

        if (daoProperties.containsKey("writeConcern")) {
            this.defaultWriteConcern = WriteConcern.valueOf((String) daoProperties.get("writeConcern"));
        } else {
            this.defaultWriteConcern = WriteConcern.SAFE;
        }

        if (Boolean.parseBoolean((String) daoProperties.get("showQueries"))) {
            queryLogger = LoggerFactory.getLogger(getClass());
        }
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(String primaryKey)
            throws NoSuchItemException {
        try {
            QueryModel queryModel = new QueryModel();
            queryModel.addCondition(buildIdCondition(primaryKey));

            if (accessControlContextProvider != null) {
                queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
            }

            return findUniqueUsingQueryModel(queryModel);
        } catch (IllegalArgumentException e) {
            throw new NoSuchItemException(getCollectionClass().getSimpleName(), primaryKey);
        }
    }


    @Override
    public final Iterable<T> findAll() {
        QueryModel queryModel = new QueryModel();

        if (accessControlContextProvider != null) {
            queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
        }

        return findUsingQueryModel(queryModel);
    }


    @Override
    public final void save(T entity) {
        saveAndReturn(entity);
    }


    @Override
    public final void delete(T entity) {
        // TODO: Probably don't want to enhance this object as we may need a previously retrieved object so
        // we can construct an appropriate identifying query w/ __olv
        DBObject dbo = (DBObject) enhancer.enhance(entity);

        DBObject identifier = createPrimaryIdentifyingQuery(dbo);

        deleteByIdentifier(identifier);
    }


    @Override
    public final void delete(String primaryKey) {
        deleteByIdentifier(createPrimaryIdentifyingQuery(primaryKey));
    }


    @Override
    public final void flush() {
        if (MongoDBSession.isActive()) {
            MongoDBSession.flush(this);
        }
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException {
        // Need to revisit te way caching works as it will miss some items...focus only on the identifier
        // instead of secondary cache keys...
        MongoDBCommand command = buildCommand(queryModel);

        if (MongoDBSession.isActive()) {
            // noinspection unchecked
            T cached = (T) MongoDBSession.getObjectFromCache(dbCollection.getName(), command.getQuery());

            if (cached != null) {
                DBObject identifier = createPrimaryIdentifyingQuery((DBObject) cached);

                MongoDBSession.trackForSave(this, identifier, cached,
                                            createIdentifyingQueries((DBObject) cached));

                return cached;
            }
        }

        // noinspection unchecked
        T result = (T) command.singleResult(dbCollection);

        if (MongoDBSession.isActive()) {
            DBObject identifier = createPrimaryIdentifyingQuery((DBObject) result);
            MongoDBSession.trackForSave(this, identifier, result,
                                        createIdentifyingQueries((DBObject) result));
        }

        return result;
    }


    public Iterable<T> findUsingQueryModel(QueryModel queryModel) {
        MongoDBCommand command = buildCommand(queryModel);
        DBCursor dbCursor = command.cursor(dbCollection);

        if (queryModel.getSorts() != null) {
            dbCursor.sort(processSorts(queryModel.getSorts()));
        }

        if (queryModel.getFirstResult() > 0) {
            dbCursor = dbCursor.skip(queryModel.getFirstResult());  // dbCursor is zero-indexed, firstResult is one-indexed
        }

        if (queryModel.getMaxResults() > 0) {
            dbCursor = dbCursor.limit(queryModel.getMaxResults());
        }

        final DBCursor finalDbCursor = dbCursor;

        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return finalDbCursor.hasNext();
                    }


                    @Override
                    @SuppressWarnings( { "unchecked" })
                    public T next() {
                        DBObject result = finalDbCursor.next();

                        ((DirtyableDBObject) result).markCurrentAsClean();

                        if (MongoDBSession.isActive()) {
                            MongoDBSession.trackForSave(MongoDBQueryModelDAO.this,
                                                        createPrimaryIdentifyingQuery(result),
                                                        (T) result,
                                                        createIdentifyingQueries(result));
                        }

                        return (T) result;
                    }


                    @Override
                    public void remove() {
                        finalDbCursor.remove();
                    }
                };
            }
        };
    }


    public Object projectUsingQueryModel(QueryModel queryModel) {
        try {
            return buildCommand(queryModel).singleResult(dbCollection);
        } catch (NoSuchItemException e) {
            return null;  // TODO: evaluate if correct
        }
    }


    @Override
    public Condition buildCondition(String conditionField,
                                    ConditionType conditionType,
                                    Iterator argsIterator) {
        MongoDBOperator mongoDBOperator = CONDITION_TYPE_TO_MONGO_OPERATOR.get(conditionType);

        if (mongoDBOperator == null) {
            throw new IllegalArgumentException("Unknown mapping for: " + conditionType);
        }

        if (conditionField.equals("id")) {
            return buildIdCondition(argsIterator.next());
        } else {
            return new Condition(conditionField, mongoDBOperator.buildConstraint(argsIterator));
        }
    }


    @Override
    public Projection buildProjection(String projectionField,
                                      ProjectionType projectionType,
                                      Iterator argsIterator) {
        return new Projection(projectionField, projectionType);
    }


    //-------------------------------------------------------------
    // Implementation - AccessControllable
    //-------------------------------------------------------------

    @Override
    public void grantAccess(String id, String accessId) {
        DBObject dbObject;

        try {
            dbObject = (DBObject) findById(id);
        } catch (NoSuchItemException e) {
            throw new RuntimeException(e);
        }

        @SuppressWarnings( { "unchecked" })
        List<String> accessControlList = (List<String>) dbObject.get(ACCESS_CONTROL_LIST_FIELD);

        if (accessControlList == null) {
            accessControlList = new ArrayList<String>();
        }

        // TODO: handle dups...
        accessControlList.add(accessId);

        dbObject.put(ACCESS_CONTROL_LIST_FIELD, accessControlList);

        //noinspection unchecked
        save((T) dbObject);
    }


    @Override
    public void revokeAccess(String id, String accessId) {
        DBObject dbObject;

        try {
            dbObject = (DBObject) findById(id);
        } catch (NoSuchItemException e) {
            throw new RuntimeException(e);
        }

        @SuppressWarnings( { "unchecked" })
        List<String> accessControlList = (List<String>) dbObject.get(ACCESS_CONTROL_LIST_FIELD);

        if (accessControlList == null) {
            return;
        }

        // TODO: verify no dups
        accessControlList.remove(accessId);

        dbObject.put(ACCESS_CONTROL_LIST_FIELD, accessControlList);

        //noinspection unchecked
        save((T) dbObject);
    }


    @Override
    public List<String> getAccessIds(String id) {
        DBObject dbObject;

        try {
            dbObject = (DBObject) findById(id);
        } catch (NoSuchItemException e) {
            throw new RuntimeException(e);
        }

        //noinspection unchecked
        return (List<String>) dbObject.get(ACCESS_CONTROL_LIST_FIELD);
    }


    @Override
    public AccessControlContextProvider getAccessControlContextProvider() {
        return accessControlContextProvider;
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected final T saveAndReturn(T entity) {
        T enhancedEntity;

        if (enhancer.needsEnhancement(entity)) {
            enhancedEntity = enhancer.enhance(entity);
        } else {
            enhancedEntity = entity;
        }

        DBObject dbo = (DBObject) enhancedEntity;

        if (dbo.get(ID_FIELD) == null) {
            dbo.put(ID_FIELD, findOrCreateIdFor(dbo));

            if (accessControlContextProvider != null) {
                if (roleAllowsAccess(accessControlContextProvider.getCurrent().getRole())
                    || accessControlContextProvider.getCurrent().getAccessId() == null) {
                    dbo.put(ACCESS_CONTROL_LIST_FIELD, Collections.<Object>emptyList());
                } else {
                    dbo.put(ACCESS_CONTROL_LIST_FIELD, Collections.singletonList(accessControlContextProvider.getCurrent().getAccessId()));
                }
            }
        }

        DBObject identifier = createPrimaryIdentifyingQuery(dbo);

        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForSave(this, identifier, enhancedEntity, createIdentifyingQueries(dbo));
        } else {
            trueSave(identifier, dbo);
        }

        return enhancedEntity;
    }


    /**
     * This method allows subclasses an opportunity to add any important data to a cache key, such
     * as the __acl from the AccessControlMongoDAO.
     * @param key key to augment
     * @return augmented key
     */
    protected DBObject augmentObjectCacheKey(DBObject key) {
        return key;
    }


    protected DBObject createPrimaryIdentifyingQuery(DBObject dbObject) {
        return createPrimaryIdentifyingQuery((ObjectId) dbObject.get("_id"));
    }


    protected DBObject[] createIdentifyingQueries(DBObject dbObject) {
        int uniqueIndexCount = uniqueIndexes.size() + 1;
        List<DBObject> queries = new ArrayList<DBObject>(uniqueIndexCount);

        queries.add(createPrimaryIdentifyingQuery(dbObject));

        for (Collection<String> indexFields : uniqueIndexes.values()) {
            DBObject query = new BasicDBObject();

            for (String indexField : indexFields) {
                query.put(indexField, getFieldValueFrom(dbObject, indexField));
            }

            queries.add(augmentObjectCacheKey(query));
        }

        return queries.toArray(new DBObject[uniqueIndexCount]);
    }


    //-------------------------------------------------------------
    // Methods - Protected - Final
    //-------------------------------------------------------------

    protected final DBObject createPrimaryIdentifyingQuery(String primaryKey) {
        if (primaryKey == null) {
            throw new IllegalArgumentException("Primary key cannot be null.");
        } else if (ObjectId.isValid(primaryKey)) {
            return createPrimaryIdentifyingQuery(new ObjectId(primaryKey));
        } else {
            return new BasicDBObject("_id", primaryKey);
        }
    }


    protected final DBObject createPrimaryIdentifyingQuery(ObjectId primaryKey) {
        return new BasicDBObject("_id", primaryKey);
    }


    protected final void deleteByIdentifier(DBObject identifier) {
        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForDelete(this, identifier);
        } else {
            trueRemove(identifier);
        }
    }


    protected final void trueSave(final DBObject identifier, final DBObject dbo) {
        // included this logic here just in case we want to enable
        // retry logic inside of 'executeAndCheckLastError' (it would
        // just need to re-run the runnable)
        if (optimisticLockEnabled) {
            Integer optimisticLockVersion = (Integer) dbo.get(OPTIMISTIC_LOCK_VERSION_FIELD);

            if (optimisticLockVersion == null) {
                dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, 1);
            } else {
                identifier.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersion);

                dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersion + 1);
            }
        }

        final DBObject optimalDbo = determineOptimalDBObject(dbo);

        if (queryLogger != null) {
            queryLogger.debug("Saving {} identified by {} with document {}",
                              new Object[] { getCollectionClass().getSimpleName(),
                                             identifier.toMap(),
                                             optimalDbo.toMap() } );
        }

        dbCollection.update(identifier, optimalDbo, true, false, getWriteConcern());

        if (DirtyableDBObject.class.isAssignableFrom(dbo.getClass())) {
            ((DirtyableDBObject) dbo).markCurrentAsClean();
        }
    }


    protected final void trueRemove(final DBObject identifier) {
        if (optimisticLockEnabled) {
            // TODO:
        }

        if (queryLogger != null) {
            queryLogger.debug("Removing {}s matching {}",
                              new Object[] { getCollectionClass().getSimpleName(),
                                             identifier.toMap() } );
        }

        dbCollection.remove(identifier, getWriteConcern());
    }


    protected final DBCollection getDbCollection() {
        return dbCollection;
    }


    protected final Class<?> getCollectionClass() {
        return enhancer.getBaseClass();
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private DBObject processSorts(List<Sort> sorts) {
        DBObject orderBy = new BasicDBObject();

        for (Sort sort : sorts) {
            orderBy.put(sort.getField(), sort.getSortDirection() == SortDirection.Ascending ? 1 : -1);
        }

        return orderBy;
    }


    // Special case for 'id' queries as it maps to _id within MongoDB.
     private Condition buildIdCondition(Object argument) {
         if (String.class.isAssignableFrom(argument.getClass())) {
             return new Condition("_id", new ObjectId((String) argument));
         } else if (Iterable.class.isAssignableFrom(argument.getClass())) {
             List<ObjectId> objectIds = new ArrayList<ObjectId>();

             for (Object argumentItem : (Iterable<?>) argument) {
                 if (argumentItem == null) {
                     objectIds.add(null);
                 } else if (argumentItem instanceof ObjectId) {
                     objectIds.add((ObjectId) argumentItem);
                 } else if (argumentItem instanceof String) {
                     objectIds.add(new ObjectId((String) argumentItem));
                 } else {
                     throw new IllegalArgumentException("Don't know how to handle class for 'id' mapping: " + argumentItem.getClass());
                 }
             }

             // TODO: create getter for MongoDBOperator.operator() ...?
             return new Condition("_id", new BasicDBObject("$in", objectIds));
         } else if (argument instanceof ObjectId) {
             return new Condition("_id", argument);
         } else {
             throw new IllegalArgumentException("Don't know how to handle class for 'id' mapping: "
                                                + argument.getClass());
         }
     }


     private ObjectId findOrCreateIdFor(DBObject dbo) {
        if (MongoDBSession.isActive()) {
            for (DBObject key : createIdentifyingQueries(dbo)) {
                if (key == null) {
                    continue;
                }
                
                Object cached = MongoDBSession.getObjectFromCache(getDbCollection().getName(), key);
                if (cached instanceof DBObject) {
                    if (((DBObject) cached).get(ID_FIELD) != null) {
                        return (ObjectId) ((DBObject) cached).get(ID_FIELD);
                    }
                }
            }
        }

        return new ObjectId();
    }


    private Object getFieldValueFrom(DBObject dbo, String field) {
        if (!field.contains(".")) {
            return dbo.get(field);
        }

        Object value = dbo;

        for (String subField : field.split(".")) {
            value = ((DBObject) value).get(subField);

            if (value == null) {
                break;
            }
        }

        return value;
    }


    private MongoDBCommand buildCommand(QueryModel queryModel) {
        BasicDBObject query = buildQueryObject(queryModel);
        MongoDBCommand command;

        if (queryModel.getProjection() == null) {
            command = new BasicDBObjectCommand(query);
        } else {
            command = ProjectionCommands.forProjection(queryModel.getProjection(), query);
        }

        if (queryLogger != null) {
            return QueryLoggingCommand.wrap(command, queryLogger);
        } else {
            return command;
        }
    }


    private Map<String, Set<String>> ensureIndexes(List<String> uniqueIndexes, final boolean unique) {
        if (uniqueIndexes == null || uniqueIndexes.size() == 0) {
            return Collections.emptyMap();
        }

        Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        for (final String uniqueIndex : uniqueIndexes) {
            final DBObject index = new BasicDBObject();
            String[] indexFields = uniqueIndex.split(",");

            for (String indexField : indexFields) {
                if (indexField.startsWith("+")) {
                    index.put(indexField.substring(1), 1);
                } else if (indexField.startsWith("-")) {
                    index.put(indexField.substring(1), -1);
                } else {
                    index.put(indexField, 1);
                }
            }

            result.put(uniqueIndex, index.keySet());

            if (queryLogger != null) {
                queryLogger.debug("Ensuring index {} on {}",
                                  new Object[] { index.toMap(), getCollectionClass().getSimpleName() } );
            }

            dbCollection.ensureIndex(index, createIndexName(uniqueIndex), unique);
        }

        return result;
    }


    private String createIndexName(String indexSpec) {
        return indexSpec.replace(',', '-');
    }


    private BasicDBObject buildQueryObject(QueryModel queryModel) {
        BasicDBObject query = new BasicDBObject();
        List<Condition> allCriteria = new ArrayList<Condition>();

        if (queryModel.getConditions() != null) {
            allCriteria.addAll(queryModel.getConditions());
        }

        for (Map.Entry<String, List<Condition>> associationConditions : queryModel.getAssociationConditions().entrySet()) {
            for (Condition condition : associationConditions.getValue()) {
                condition.setField(associationConditions.getKey() + "." + condition.getField());

                allCriteria.add(condition);
            }
        }

        // TODO: optimize this -- iterating over everything above and again here
        for (Condition condition : allCriteria) {
            // we need to ensure that all condition objects are mongodb-safe
            // examples of "unsafe" things are: Sets, Enum, POJOs.
            Object rawConstraint = condition.getConstraint();
            Object constraint = (rawConstraint == null) ? null
                                                        : DBObjectUtil.toDBObject(rawConstraint.getClass(), rawConstraint);

            // XXX : if annotation specifies multiple conditions on single field the
            // first condition will be overwritten here
            query.put(condition.getField(), constraint);
        }

        if (accessControlContextProvider != null) {
            if (!roleAllowsAccess(queryModel.getAccessControlContext().getRole())) {
                // NB: will match any item w/in the ACL list.
                query.put(ACCESS_CONTROL_LIST_FIELD, queryModel.getAccessControlContext().getAccessId());
            }
        }

        return query;
    }


    private DBObject determineOptimalDBObject(DBObject dbo) {
        // TODO: handle saveNulls...

        return dbo;
    }


    private WriteConcern getWriteConcern() {
        // TODO: Add ability to swap a concern out on a per-call basis...if nothing overwrites/changes, then
        // return the default.

        return defaultWriteConcern;
    }


    private boolean roleAllowsAccess(String role) {
        AccessControl accessControl;

        if (role == null || role.isEmpty()) {
            return false;
        }

        if ((accessControl = getAccessControlAnnotation()) == null) {
            return false;
        }

        for (AccessControlRule accessControlRule : accessControl.rules()) {
            if (accessControlRule.type() == AccessControlType.Role && accessControlRule.value().equals(role)) {
                return true;
            }
        }

        return false;
    }


    private AccessControl getAccessControlAnnotation() {
        Class collectionClass = getCollectionClass();

        while (collectionClass != null) {
            // noinspection unchecked
            AccessControl accessControl = (AccessControl) collectionClass.getAnnotation(AccessControl.class);

            if (accessControl != null) {
                return accessControl;
            }

            collectionClass = collectionClass.getSuperclass();
        }

        return null;
    }
}
