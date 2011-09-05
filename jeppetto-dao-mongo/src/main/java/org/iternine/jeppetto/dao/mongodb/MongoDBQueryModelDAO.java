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

package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.AccessControllable;
import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.ConditionType;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.Projection;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.QueryModel;
import org.iternine.jeppetto.dao.QueryModelDAO;
import org.iternine.jeppetto.dao.Sort;
import org.iternine.jeppetto.dao.SortDirection;
import org.iternine.jeppetto.dao.annotation.AccessControl;
import org.iternine.jeppetto.dao.annotation.AccessControlRule;
import org.iternine.jeppetto.dao.annotation.AccessControlType;
import org.iternine.jeppetto.dao.mongodb.enhance.DBObjectUtil;
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObject;
import org.iternine.jeppetto.dao.mongodb.enhance.EnhancerHelper;
import org.iternine.jeppetto.dao.mongodb.enhance.MongoDBCallback;
import org.iternine.jeppetto.dao.mongodb.projections.ProjectionCommands;
import org.iternine.jeppetto.enhance.Enhancer;

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
 * <p/>
 * This QueryModelDAO implementation for MongoDB provides bi-directional ODM (Object to Document Mapping) as well
 * as the rich query capabilities found in the core org.iternine.jeppetto.dao package.  It requires that the ID be a String.
 *
 * Instantiation requires a daoProperties Map<String, Object> that will look for the following keys (and expected
 * values):
 * <p/>
 * <table>
 *   <tr>
 *     <td>Key</td>
 *     <td>Required?</td>
 *     <td>Description</td>
 *   </tr>
 *   <tr>
 *     <td>db</td>
 *     <td>Yes</td>
 *     <td>Instance of a configured com.mongodb.DB</td>
 *   </tr>
 *   <tr>
 *     <td>uniqueIndexes</td>
 *     <td>No</td>
 *     <td>List<String> of various MongoDB index values that will be ensured to exist and must be unique.</td>
 *   </tr>
 *   <tr>
 *     <td>nonUniqueIndexes</td>
 *     <td>No</td>
 *     <td>List<String> of various MongoDB index values that will be ensured to exist and need not be unique.</td>
 *   </tr>
 *   <tr>
 *     <td>optimisticLockEnabled</td>
 *     <td>No</td>
 *     <td>Boolean to indicate if instances of the tracked type should be protected by a Jeppetto-managed lock version field.</td>
 *   </tr>
 *   <tr>
 *     <td>shardKeyPattern</td>
 *     <td>No</td>
 *     <td>A comma-separated string of fields that are used to determine the shard key(s) for the collection.</td>
 *   </tr>
 *   <tr>
 *     <td>saveNulls</td>
 *     <td>No</td>
 *     <td>Boolean to indicate whether null fields should be included in MongoDB documents.</td>
 *   </tr>
 *   <tr>
 *     <td>writeConcern</td>
 *     <td>No</td>
 *     <td>String, one of the values as indicated at http://www.mongodb.org/display/DOCS/Replica+Set+Semantics.  If
 *         not specified, the DAO defaults to "SAFE".</td>
 *   </tr>
 *   <tr>
 *     <td>showQueries</td>
 *     <td>No</td>
 *     <td>Boolean to indicate if executed queries should be logged.  Note that logging will need to be enabled for
 *         the DAO's package as well.</td>
 *   </tr>
 * </table>
 * @param <T> the type of persistent object this DAO will manage.
 */
// TODO: support createdDate/lastModifiedDate (createdDate from get("_id").getTime()?)
// TODO: Implement determineOptimalDBObject
//          - understand saveNulls
//          - understand field-level deltas
// TODO: support per-call WriteConcerns (keep in mind session semantics)
// TODO: investigate usage of ClassLoader so new instances are already enhanced
public class MongoDBQueryModelDAO<T, ID>
        implements QueryModelDAO<T, ID>, AccessControllable<ID> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final String ID_FIELD = "_id";
    private static final String OPTIMISTIC_LOCK_VERSION_FIELD = "__olv";
    private static final String ACCESS_CONTROL_LIST_FIELD = "__acl";


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private DBCollection dbCollection;
    private Enhancer<T> enhancer;
    private AccessControlContextProvider accessControlContextProvider;
    private Map<String, Set<String>> uniqueIndexes;
    private boolean optimisticLockEnabled;
    private List<String> shardKeys;
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
        this.shardKeys = extractShardKeys((String) daoProperties.get("shardKeyPattern"));
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
    public T findById(ID id)
            throws NoSuchItemException {
        try {
            QueryModel queryModel = new QueryModel();
            queryModel.addCondition(buildIdCondition(id));

            if (accessControlContextProvider != null) {
                queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
            }

            return findUniqueUsingQueryModel(queryModel);
        } catch (IllegalArgumentException e) {
            throw new NoSuchItemException(getCollectionClass().getSimpleName(), id.toString());
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
        T enhancedEntity = enhancer.enhance(entity);
        DirtyableDBObject dbo = (DirtyableDBObject) enhancedEntity;

        if (!dbo.isPersisted()) {
            if (dbo.get(ID_FIELD) == null) {
                dbo.put(ID_FIELD, new ObjectId());  // If the id isn't explicitly set, assume intent is for mongo ids
            }

            if (accessControlContextProvider != null) {
                if (roleAllowsAccess(accessControlContextProvider.getCurrent().getRole())
                    || accessControlContextProvider.getCurrent().getAccessId() == null) {
                    dbo.put(ACCESS_CONTROL_LIST_FIELD, Collections.<Object>emptyList());
                } else {
                    dbo.put(ACCESS_CONTROL_LIST_FIELD, Collections.singletonList(accessControlContextProvider.getCurrent().getAccessId()));
                }
            }
        }

        DBObject identifyingQuery = buildIdentifyingQuery(dbo);

        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForSave(this, identifyingQuery, enhancedEntity, createIdentifyingQueries(dbo));
        } else {
            trueSave(identifyingQuery, dbo);
        }
    }


    @Override
    public final void delete(T entity) {
        // TODO: Probably don't want to enhance this object as we may need a previously retrieved object so
        // we can construct an appropriate identifying query w/ __olv
        DBObject dbo = (DBObject) enhancer.enhance(entity);

        DBObject identifyingQuery = buildIdentifyingQuery(dbo);

        for (String shardKey : shardKeys) {
            identifyingQuery.put(shardKey, dbo.get(shardKey));
        }

        deleteByIdentifyingQuery(identifyingQuery);
    }


    @Override
    public final void deleteById(ID id) {
        deleteByIdentifyingQuery(buildIdentifyingQuery(id));
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
        // Need to revisit te way caching works as it will miss some items...focus only on the identifyingQuery
        // instead of secondary cache keys...
        MongoDBCommand command = buildCommand(queryModel);

        if (MongoDBSession.isActive()) {
            // noinspection unchecked
            T cached = (T) MongoDBSession.getObjectFromCache(dbCollection.getName(), command.getQuery());

            if (cached != null) {
                DBObject identifyingQuery = buildIdentifyingQuery((DBObject) cached);

                MongoDBSession.trackForSave(this, identifyingQuery, cached, createIdentifyingQueries((DBObject) cached));

                return cached;
            }
        }

        // noinspection unchecked
        T result = (T) command.singleResult(dbCollection);

        if (MongoDBSession.isActive()) {
            DBObject identifyingQuery = buildIdentifyingQuery((DBObject) result);

            MongoDBSession.trackForSave(this, identifyingQuery, result, createIdentifyingQueries((DBObject) result));
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

                        ((DirtyableDBObject) result).markPersisted();

                        if (MongoDBSession.isActive()) {
                            MongoDBSession.trackForSave(MongoDBQueryModelDAO.this,
                                                        buildIdentifyingQuery(result),
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
        if (conditionField.equals("id")) {
            return buildIdCondition(argsIterator.next());
        } else {
            return new Condition(conditionField,
                                 MongoDBOperator.valueOf(conditionType.name()).buildConstraint(argsIterator));
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
    public void grantAccess(ID id, String accessId) {
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
    public void revokeAccess(ID id, String accessId) {
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
    public List<String> getAccessIds(ID id) {
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

    /**
     * This method allows subclasses an opportunity to add any important data to a cache key, such
     * as the __acl from the AccessControlMongoDAO.
     *
     * TODO: revsit these methods...
     *
     * @param key key to augment
     * @return augmented key
     */
    protected DBObject augmentObjectCacheKey(DBObject key) {
        return key;
    }


    protected DBObject[] createIdentifyingQueries(DBObject dbObject) {
        int uniqueIndexCount = uniqueIndexes.size() + 1;
        List<DBObject> queries = new ArrayList<DBObject>(uniqueIndexCount);

        queries.add(buildIdentifyingQuery(dbObject));

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

    protected final DBObject buildIdentifyingQuery(DBObject dbObject) {
        //noinspection unchecked
        return buildIdentifyingQuery((ID) dbObject.get(ID_FIELD));
    }


    protected final DBObject buildIdentifyingQuery(ID id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null.");
        } else if (id instanceof String && ObjectId.isValid((String) id)) {
            return new BasicDBObject("_id", new ObjectId((String) id));
        } else {
            return new BasicDBObject("_id", id);
        }
    }


    protected final void deleteByIdentifyingQuery(DBObject identifyingQuery) {
        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForDelete(this, identifyingQuery);
        } else {
            trueRemove(identifyingQuery);
        }
    }


    protected final void trueSave(final DBObject identifyingQuery, final DirtyableDBObject dbo) {
        if (optimisticLockEnabled) {
            Integer optimisticLockVersion = (Integer) dbo.get(OPTIMISTIC_LOCK_VERSION_FIELD);

            if (optimisticLockVersion == null) {
                dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, 1);
            } else {
                // TODO: should this modification of identifyingQuery been done earlier (in save())?
                identifyingQuery.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersion);

                dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersion + 1);
            }
        }

        for (String shardKey : shardKeys) {
            identifyingQuery.put(shardKey, dbo.get(shardKey));
        }

        final DBObject optimalDbo = determineOptimalDBObject(dbo);

        if (queryLogger != null) {
            queryLogger.debug("Saving {} identified by {} with document {}",
                              new Object[] { getCollectionClass().getSimpleName(),
                                             identifyingQuery.toMap(),
                                             optimalDbo.toMap() } );
        }

        dbCollection.update(identifyingQuery, optimalDbo, true, false, getWriteConcern());

        dbo.markPersisted();
    }


    protected final void trueRemove(final DBObject identifyingQuery) {
        if (optimisticLockEnabled) {
            // TODO:
        }

        if (queryLogger != null) {
            queryLogger.debug("Removing {}s matching {}",
                              new Object[] { getCollectionClass().getSimpleName(), identifyingQuery.toMap() } );
        }

        dbCollection.remove(identifyingQuery, getWriteConcern());
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
         if (argument instanceof String && ObjectId.isValid((String) argument)) {
             return new Condition("_id", new ObjectId((String) argument));
         } else if (Iterable.class.isAssignableFrom(argument.getClass())) {
             List<ObjectId> objectIds = new ArrayList<ObjectId>();

             //noinspection ConstantConditions
             for (Object argumentItem : (Iterable) argument) {
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
         } else {
             return new Condition("_id", argument);
         }
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
                indexField = indexField.trim();

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


    private List<String> extractShardKeys(String shardKeyPattern) {
        if (shardKeyPattern == null) {
            return Collections.emptyList();
        }

        String[] shardKeyParts = shardKeyPattern.split(",");
        List<String> shardKeys = new ArrayList<String>(shardKeyParts.length);

        for (String shardKey : shardKeyParts) {
            shardKey = shardKey.trim();

            if (shardKey.equals("id") || shardKey.equals("_id")) {
                continue;
            }

            shardKeys.add(shardKey);
        }

        return shardKeys;
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
