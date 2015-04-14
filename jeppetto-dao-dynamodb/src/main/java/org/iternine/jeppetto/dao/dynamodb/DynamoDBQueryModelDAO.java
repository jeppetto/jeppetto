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

package org.iternine.jeppetto.dao.dynamodb;


import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.ConditionType;
import org.iternine.jeppetto.dao.FailedBatchException;
import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.OptimisticLockException;
import org.iternine.jeppetto.dao.Pair;
import org.iternine.jeppetto.dao.Projection;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.QueryModel;
import org.iternine.jeppetto.dao.QueryModelDAO;
import org.iternine.jeppetto.dao.Sort;
import org.iternine.jeppetto.dao.SortDirection;
import org.iternine.jeppetto.dao.TooManyItemsException;
import org.iternine.jeppetto.dao.UpdateBehaviorDescriptor;
import org.iternine.jeppetto.dao.ResultFromUpdate;
import org.iternine.jeppetto.dao.dynamodb.expression.ConditionExpressionBuilder;
import org.iternine.jeppetto.dao.dynamodb.expression.ProjectionExpressionBuilder;
import org.iternine.jeppetto.dao.dynamodb.expression.UpdateExpressionBuilder;
import org.iternine.jeppetto.dao.dynamodb.iterable.BatchGetIterable;
import org.iternine.jeppetto.dao.dynamodb.iterable.DynamoDBIterable;
import org.iternine.jeppetto.dao.dynamodb.iterable.QueryIterable;
import org.iternine.jeppetto.dao.dynamodb.iterable.ScanIterable;
import org.iternine.jeppetto.dao.id.IdGenerator;
import org.iternine.jeppetto.dao.updateobject.UpdateObject;
import org.iternine.jeppetto.enhance.Enhancer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of the QueryModelDAO interface that works atop DynamoDB.
 *
 * @param <T> Persistent class
 * @param <ID> ID type of the persistent class.
 */
public class DynamoDBQueryModelDAO<T, ID>
        implements QueryModelDAO<T, ID> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBQueryModelDAO.class);


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final Class<T> entityClass;
    private final AmazonDynamoDB dynamoDB;
    private final String tableName;
    private final IdGenerator<ID> idGenerator;
    private final boolean consistentRead;
    private final String optimisticLockField;
    private final boolean enableScans;

    private final String hashKeyField;
    private final String rangeKeyField;
    private final String projectionExpression;
    private final Map<String, String> projectionExpressionNames;
    private final Map<String, Map<String, IndexData>> indexes;
    private final Map<String, Map<String, IndexData>> baseIndexOnly;
    private final Enhancer<T> persistableEnhancer;
    private final Enhancer<? extends T> updateObjectEnhancer;
    private final String uniqueIdConditionExpression;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected DynamoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties) {
        this(entityClass, daoProperties, null);
    }


    @SuppressWarnings({ "unchecked", "UnusedParameters" })
    protected DynamoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties,
                                    AccessControlContextProvider accessControlContextProvider) {
        this.entityClass = entityClass;
        this.dynamoDB = (AmazonDynamoDB) daoProperties.get("db");
        this.tableName = daoProperties.containsKey("tableName") ? (String) daoProperties.get("tableName") : entityClass.getSimpleName();
        this.idGenerator = (IdGenerator<ID>) daoProperties.get("idGenerator");
        this.consistentRead = Boolean.parseBoolean((String) daoProperties.get("consistentRead"));   // null okay - defaults to false
        this.optimisticLockField = (String) daoProperties.get("optimisticLockField");
        this.enableScans = Boolean.parseBoolean((String) daoProperties.get("enableScans"));             // null okay - defaults to false

        TableDescription tableDescription = dynamoDB.describeTable(tableName).getTable();

        Pair<String, String> primaryKeyAttributeNames = getKeyAttributeNames(tableDescription.getKeySchema());
        this.hashKeyField = primaryKeyAttributeNames.getFirst();
        this.rangeKeyField = primaryKeyAttributeNames.getSecond();

        ProjectionExpressionBuilder projectionExpressionBuilder;
        if (Boolean.parseBoolean((String) daoProperties.get("projectionObject"))) {   // null okay - defaults to false
            projectionExpressionBuilder = new ProjectionExpressionBuilder(entityClass, hashKeyField, rangeKeyField, optimisticLockField);
            this.projectionExpression = projectionExpressionBuilder.getExpression();
            this.projectionExpressionNames = projectionExpressionBuilder.getExpressionAttributeNames();
        } else {
            projectionExpressionBuilder = null;
            this.projectionExpression = null;
            this.projectionExpressionNames = Collections.emptyMap();
        }

        List<String> keyFields = rangeKeyField == null ? Collections.singletonList(hashKeyField) : Arrays.asList(hashKeyField, rangeKeyField);
        IndexData baseIndexData = new IndexData(null, keyFields, true);
        this.baseIndexOnly = Collections.singletonMap(hashKeyField, Collections.singletonMap(rangeKeyField, baseIndexData));
        this.indexes = processIndexes(tableDescription, projectionExpressionBuilder, baseIndexData);
        this.persistableEnhancer = EnhancerHelper.getPersistableEnhancer(entityClass);

        String updateObjectClassName = (String) daoProperties.get("updateObject");
        if (updateObjectClassName == null) {
            this.updateObjectEnhancer = EnhancerHelper.getUpdateObjectEnhancer(entityClass);
        } else {
            try {
                Class updateObjectClass = Class.forName(updateObjectClassName);

                if (!entityClass.isAssignableFrom(updateObjectClass)) {
                    throw new JeppettoException(String.format("Invalid UpdateObject type. %s does not subclass entity type %s",
                                                              updateObjectClassName, entityClass.getName()));
                }

                this.updateObjectEnhancer = (Enhancer<? extends T>) EnhancerHelper.getUpdateObjectEnhancer(updateObjectClass);
            } catch (ClassNotFoundException e) {
                throw new JeppettoException(e);
            }
        }

        if (Boolean.parseBoolean((String) daoProperties.get("verifyUniqueIds"))) {   // null okay - defaults to false
            ConditionExpressionBuilder conditionExpressionBuilder
                    = new ConditionExpressionBuilder().with(hashKeyField, new DynamoDBConstraint(DynamoDBOperator.IsNull));

            if (rangeKeyField != null) {
                conditionExpressionBuilder.with(rangeKeyField, new DynamoDBConstraint(DynamoDBOperator.IsNull));
            }

            this.uniqueIdConditionExpression = conditionExpressionBuilder.getExpression();    // No attribute values needed
        } else {
            this.uniqueIdConditionExpression = null;
        }
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(ID id)
            throws NoSuchItemException, JeppettoException {
        GetItemResult result;

        try {
            GetItemRequest getItemRequest = new GetItemRequest(tableName, getKeyFrom(id));

            getItemRequest.setProjectionExpression(projectionExpression);

            if (!projectionExpressionNames.isEmpty()) {
                getItemRequest.setExpressionAttributeNames(projectionExpressionNames);
            }

            result = dynamoDB.getItem(getItemRequest);
        } catch (AmazonClientException e) {
            throw new JeppettoException(e);
        }

        if (result.getItem() == null) {
            throw new NoSuchItemException(entityClass.getSimpleName(), id.toString());
        }

        T t = ConversionUtil.getObjectFromItem(result.getItem(), entityClass);

        ((DynamoDBPersistable) t).__markPersisted(dynamoDB.toString());

        return t;
    }


    @Override
    public Iterable<T> findByIds(ID... ids)
            throws JeppettoException {
        Collection<Map<String, AttributeValue>> keys = new ArrayList<Map<String, AttributeValue>>();

        for (ID id : ids) {
            keys.add(getKeyFrom(id));
        }

        KeysAndAttributes keysAndAttributes = new KeysAndAttributes().withKeys(keys);

        keysAndAttributes.setProjectionExpression(projectionExpression);

        if (!projectionExpressionNames.isEmpty()) {
            keysAndAttributes.setExpressionAttributeNames(projectionExpressionNames);
        }

        BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(tableName, keysAndAttributes));

        return new BatchGetIterable<T>(dynamoDB, persistableEnhancer, batchGetItemRequest, tableName);
    }


    @Override
    public Iterable<T> findAll()
            throws JeppettoException {
        return findUsingQueryModel(new QueryModel());
    }


    @Override
    public void save(T entity)
            throws OptimisticLockException, JeppettoException {
        DynamoDBPersistable dynamoDBPersistable = (DynamoDBPersistable) persistableEnhancer.enhance(entity);

        if (!dynamoDBPersistable.__isPersisted(dynamoDB.toString())) {
            if (optimisticLockField != null) {
                dynamoDBPersistable.__put(optimisticLockField, new AttributeValue().withN("0"));
            }

            saveItem(dynamoDBPersistable);
        } else {
            ConditionExpressionBuilder conditionExpressionBuilder;

            if (optimisticLockField != null) {
                AttributeValue attributeValue = (AttributeValue) dynamoDBPersistable.__get(optimisticLockField);
                int optimisticLockVersion;

                if (attributeValue != null) {
                    optimisticLockVersion = Integer.parseInt(attributeValue.getN());

                    conditionExpressionBuilder = new ConditionExpressionBuilder();

                    conditionExpressionBuilder.with(optimisticLockField, new DynamoDBConstraint(DynamoDBOperator.Equal, optimisticLockVersion));
                } else {
                    optimisticLockVersion = -1;

                    conditionExpressionBuilder = null;
                }

                dynamoDBPersistable.__put(optimisticLockField, new AttributeValue().withN(Integer.toString(optimisticLockVersion + 1)));
            } else {
                conditionExpressionBuilder = null;
            }

            try {
                UpdateExpressionBuilder updateExpressionBuilder = new UpdateExpressionBuilder(dynamoDBPersistable);

                updateItem(getKeyFrom(dynamoDBPersistable), updateExpressionBuilder, conditionExpressionBuilder, ResultFromUpdate.ReturnNone);
            } catch (JeppettoException e) {
                if (optimisticLockField != null && e.getCause() instanceof ConditionalCheckFailedException) {
                    throw new OptimisticLockException(e.getCause());
                } else {
                    throw e;
                }
            }
        }

        dynamoDBPersistable.__markPersisted(dynamoDB.toString());
    }


    @Override
    public void delete(T entity)
            throws JeppettoException {
        if (entity == null) {
            throw new JeppettoException("entity is null; nothing to delete.");
        }

        deleteItem(getKeyFrom((DynamoDBPersistable) persistableEnhancer.enhance(entity)));
    }


    @Override
    public void deleteById(ID id)
            throws JeppettoException {
        if (id == null) {
            throw new JeppettoException("id is null; unable to delete entity.");
        }

        deleteItem(getKeyFrom(id));
    }


    @Override
    public void deleteByIds(ID... ids)
            throws FailedBatchException, JeppettoException {
        List<ID> succeeded = new ArrayList<ID>();
        Map<ID, Exception> failed = new LinkedHashMap<ID, Exception>();

        for (ID id : ids) {
            try {
                deleteItem(getKeyFrom(id));

                succeeded.add(id);
            } catch (Exception e) {
                //noinspection ThrowableResultOfMethodCallIgnored
                failed.put(id, e);
            }
        }

        if (failed.size() > 0) {
            throw new FailedBatchException("Unable to delete all items", succeeded, failed);
        }
    }


    @Override
    public <U extends T> U getUpdateObject() {
        //noinspection unchecked
        return (U) updateObjectEnhancer.newInstance();
    }


    @Override
    public <U extends T> T updateById(U updateObject, ID id)
            throws JeppettoException {
        return updateItem(getKeyFrom(id), new UpdateExpressionBuilder((UpdateObject) updateObject), null, getResultFromUpdate(updateObject));
    }


    @Override
    public <U extends T> Iterable<T> updateByIds(U updateObject, ID... ids)
            throws FailedBatchException, JeppettoException {
        List<?> succeeded;
        Map<ID, Exception> failed = new LinkedHashMap<ID, Exception>();
        ResultFromUpdate resultFromUpdate = getResultFromUpdate(updateObject);

        if (resultFromUpdate == ResultFromUpdate.ReturnNone) {
            succeeded = new ArrayList<ID>();
        } else {
            succeeded = new ArrayList<T>();
        }

        UpdateExpressionBuilder updateExpressionBuilder = new UpdateExpressionBuilder((UpdateObject) updateObject);
        for (ID id : ids) {
            try {
                T t = updateItem(getKeyFrom(id), updateExpressionBuilder, null, resultFromUpdate);

                if (resultFromUpdate == ResultFromUpdate.ReturnNone) {
                    //noinspection unchecked
                    ((List<ID>) succeeded).add(id);
                } else {
                    //noinspection unchecked
                    ((List<T>) succeeded).add(t);
                }
            } catch (Exception e) {
                //noinspection ThrowableResultOfMethodCallIgnored
                failed.put(id, e);
            }
        }

        if (failed.size() > 0) {
            throw new FailedBatchException("Unable to update all items", succeeded, failed);
        }

        //noinspection unchecked
        return resultFromUpdate == ResultFromUpdate.ReturnNone ? null : (Iterable<T>) succeeded;
    }


    @Override
    public void flush()
            throws JeppettoException {
        // Flush not required.
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    @Override
    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException, TooManyItemsException, JeppettoException {
        DynamoDBIterable<T> dynamoDBIterable = (DynamoDBIterable<T>) findUsingQueryModel(queryModel);

        dynamoDBIterable.setLimit(1);

        Iterator<T> results = dynamoDBIterable.iterator();

        if (!results.hasNext()) {
            throw new NoSuchItemException();
        }

        T result = results.next();

        if (dynamoDBIterable.hasResultsPastLimit()) {
            throw new TooManyItemsException();
        }

        return result;
    }


    @Override
    public Iterable<T> findUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        ConditionExpressionBuilder conditionExpressionBuilder = new ConditionExpressionBuilder(queryModel, indexes);

        if (conditionExpressionBuilder.hasHashKeyCondition()) {
            return queryItems(queryModel, conditionExpressionBuilder);
        } else if (enableScans) {
            logger.info("Condition does not specify a hash key -- using 'scan' to search.");

            conditionExpressionBuilder.convertRangeKeyConditionToExpression();

            return scanItems(queryModel, conditionExpressionBuilder);
        } else {
            throw new JeppettoException("Find cannot be satisfied without a scan and scans have not been enabled."
                                        + "  Configure this DAO with 'enableScans' = true to allow this.");
        }
    }


    @Override
    public Object projectUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        // TODO: handle count case.  not sure about other projections...

        throw new UnsupportedOperationException("Projections on DynamoDB are not currently supported.");
    }


    @Override
    public void deleteUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        Iterable<T> matches = findUsingQueryModel(queryModel);

        // Would ideally catch individual exceptions and throw a FailedBatchException.  Unfortunately, we don't have an easy
        // way to convert the key from a match to an ID object, which is what the exception's Map contains.  Maybe DynamoDB will
        // add support for a query-based delete.
        for (T match : matches) {
            delete(match);
        }
    }


    @Override
    public <U extends T> T updateUniqueUsingQueryModel(U updateObject, QueryModel queryModel)
            throws JeppettoException {
        UpdateExpressionBuilder updateExpressionBuilder = new UpdateExpressionBuilder((UpdateObject) updateObject);
        // For referencing an object, we can only identify an item by its actual range key, not one of the index fields.
        ConditionExpressionBuilder conditionExpressionBuilder = new ConditionExpressionBuilder(queryModel, baseIndexOnly);
        ResultFromUpdate resultFromUpdate = getResultFromUpdate(updateObject);
        Map<String, AttributeValue> key;

        try {
            key = conditionExpressionBuilder.getKey();
        } catch (NullPointerException e) {
            throw new JeppettoException("DynamoDB only supports updates where the condition uniquely identifies the item by its key.", e);
        }

        return updateItem(key, updateExpressionBuilder, conditionExpressionBuilder, resultFromUpdate);
    }


    @Override
    public <U extends T> Iterable<T> updateUsingQueryModel(U updateObject, QueryModel queryModel)
            throws JeppettoException {
        // DynamoDB only supports updating a single item at a time.
        T t = updateUniqueUsingQueryModel(updateObject, queryModel);

        if (t == null) {
            return null;
        }

        return Collections.singletonList(t);
    }


    @Override
    public Condition buildCondition(String conditionField, ConditionType conditionType, Iterator argsIterator) {
        return new Condition(conditionField, DynamoDBOperator.valueOf(conditionType.name()).buildConstraint(argsIterator));
    }


    @Override
    public Projection buildProjection(String projectionField, ProjectionType projectionType, Iterator argsIterator) {
        throw new UnsupportedOperationException("Projections on DynamoDB are not currently supported.");
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private Map<String, Map<String, IndexData>> processIndexes(TableDescription tableDescription,
                                                               ProjectionExpressionBuilder projectionExpressionBuilder,
                                                               IndexData baseIndexData) {
        // Collect information about the local secondary indexes.  These will be included with the global indexes below.
        Map<String, IndexData> localIndexes;
        List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription.getLocalSecondaryIndexes();
        if (localSecondaryIndexes != null) {
            localIndexes = new HashMap<String, IndexData>(localSecondaryIndexes.size() + 2);

            // We include these as local indexes to make findUsingQueryModel() code below simpler
            localIndexes.put(rangeKeyField, baseIndexData);
            localIndexes.put(null, baseIndexData);

            for (LocalSecondaryIndexDescription description : localSecondaryIndexes) {
                String indexField = getKeyAttributeNames(description.getKeySchema()).getSecond();
                boolean projectsOverEntity = description.getProjection().getProjectionType().equals("ALL")
                                             || projectionExpressionBuilder != null
                                                && projectionExpressionBuilder.isCoveredBy(description.getProjection());

                List<String> keyFields = new ArrayList<String>(baseIndexData.keyFields);
                keyFields.add(indexField);

                localIndexes.put(indexField, new IndexData(description.getIndexName(), keyFields, projectsOverEntity));
            }
        } else if (rangeKeyField != null) {
            localIndexes = new HashMap<String, IndexData>(2);

            localIndexes.put(rangeKeyField, baseIndexData);
            localIndexes.put(null, baseIndexData);
        } else {
            localIndexes = Collections.singletonMap(null, baseIndexData);
        }

        // Process the global secondary indexes.  When done, add the local index information.
        List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = tableDescription.getGlobalSecondaryIndexes();
        if (globalSecondaryIndexes != null) {
            Map<String, Map<String, IndexData>> indexes = new HashMap<String, Map<String, IndexData>>(globalSecondaryIndexes.size() + 1);

            for (GlobalSecondaryIndexDescription description : globalSecondaryIndexes) {
                Pair<String, String> indexFields = getKeyAttributeNames(description.getKeySchema());
                boolean projectsOverEntity = description.getProjection().getProjectionType().equals("ALL")
                                             || projectionExpressionBuilder != null
                                                && projectionExpressionBuilder.isCoveredBy(description.getProjection());

                List<String> keyFields = new ArrayList<String>();
                keyFields.add(indexFields.getFirst());
                if (indexFields.getSecond() != null) {
                    keyFields.add(indexFields.getSecond());
                }
                keyFields.add(hashKeyField);
                if (rangeKeyField != null) {
                    keyFields.add(rangeKeyField);
                }

                IndexData indexData = new IndexData(description.getIndexName(), keyFields, projectsOverEntity);

                if (!indexes.containsKey(indexFields.getFirst())) {
                    indexes.put(indexFields.getFirst(), new HashMap<String, IndexData>());
                }

                indexes.get(indexFields.getFirst()).put(indexFields.getSecond(), indexData);

                // In case a query doesn't specify a range key, we still want to select an index for this hash key.
                // If one has already been selected, pick one that projects over this entity to avoid extra DB reads.
                IndexData noRangeKeyIndexData = indexes.get(indexFields.getFirst()).get(null);
                if (noRangeKeyIndexData == null || !noRangeKeyIndexData.projectsOverEntity) {
                    indexes.get(indexFields.getFirst()).put(null, indexData);
                }
            }

            indexes.put(hashKeyField, localIndexes);

            return indexes;
        } else {
            return Collections.singletonMap(hashKeyField, localIndexes);
        }
    }


    private void saveItem(DynamoDBPersistable dynamoDBPersistable) {
        generateIdIfNeeded(dynamoDBPersistable);

        try {
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName)
                                                                .withItem(ConversionUtil.getItemFromObject(dynamoDBPersistable))
                                                                .withConditionExpression(uniqueIdConditionExpression);

            dynamoDB.putItem(putItemRequest);
        } catch (Exception e) {
            throw new JeppettoException(e);
        }
    }


    private T updateItem(Map<String, AttributeValue> key, UpdateExpressionBuilder updateExpressionBuilder,
                         ConditionExpressionBuilder conditionExpressionBuilder, ResultFromUpdate resultFromUpdate) {
        try {
            UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName)
                                                                         .withKey(key)
                                                                         .withUpdateExpression(updateExpressionBuilder.getExpression());

            Map<String, AttributeValue> expressionAttributeValues;
            Map<String, String> expressionAttributeNames;

            if (conditionExpressionBuilder == null) {
                expressionAttributeValues = updateExpressionBuilder.getExpressionAttributeValues();
                expressionAttributeNames = updateExpressionBuilder.getExpressionAttributeNames();
            } else {
                expressionAttributeValues = new LinkedHashMap<String, AttributeValue>();
                expressionAttributeNames = new LinkedHashMap<String, String>();

                expressionAttributeValues.putAll(updateExpressionBuilder.getExpressionAttributeValues());
                expressionAttributeNames.putAll(updateExpressionBuilder.getExpressionAttributeNames());

                expressionAttributeValues.putAll(conditionExpressionBuilder.getExpressionAttributeValues());
                expressionAttributeNames.putAll(conditionExpressionBuilder.getExpressionAttributeNames());

                updateItemRequest.setConditionExpression(conditionExpressionBuilder.getExpression());
            }

            if (!expressionAttributeValues.isEmpty()) {
                updateItemRequest.setExpressionAttributeValues(expressionAttributeValues);
            }

            if (!expressionAttributeNames.isEmpty()) {
                updateItemRequest.setExpressionAttributeNames(expressionAttributeNames);
            }

            if (resultFromUpdate != ResultFromUpdate.ReturnNone) {
                updateItemRequest.setReturnValues(resultFromUpdate == ResultFromUpdate.ReturnPreUpdate ? ReturnValue.ALL_OLD
                                                                                                       : ReturnValue.ALL_NEW);

                UpdateItemResult result = dynamoDB.updateItem(updateItemRequest);

                T t = ConversionUtil.getObjectFromItem(result.getAttributes(), entityClass);

                ((DynamoDBPersistable) t).__markPersisted(dynamoDB.toString());

                return t;
            } else {
                dynamoDB.updateItem(updateItemRequest);

                return null;
            }
        } catch (Exception e) {
            throw new JeppettoException(e);
        }
    }


    private void deleteItem(Map<String, AttributeValue> key) {
        try {
            dynamoDB.deleteItem(new DeleteItemRequest(tableName, key));
        } catch (Exception e) {
            throw new JeppettoException(e);
        }
    }


    private Iterable<T> queryItems(QueryModel queryModel, ConditionExpressionBuilder conditionExpressionBuilder) {
        QueryRequest queryRequest = new QueryRequest(tableName);

        queryRequest.setKeyConditions(conditionExpressionBuilder.getKeyConditions());
        queryRequest.setConsistentRead(consistentRead);

        if (queryModel.getFirstResult() > 0) {
            logger.warn("DynamoDB does not support skipping results.  Call setPosition() on DynamoDBIterable instead.");
        }

        if (queryModel.getMaxResults() > 0) {
            queryRequest.setLimit(queryModel.getMaxResults());
        }

        List<String> keyFields = applyIndexAndGetKeyFields(conditionExpressionBuilder, queryRequest, queryModel.getSorts());
        applyExpressions(conditionExpressionBuilder, queryRequest);

        return new QueryIterable<T>(dynamoDB, persistableEnhancer, queryRequest, keyFields.get(0), keyFields);
    }


    private List<String> applyIndexAndGetKeyFields(ConditionExpressionBuilder conditionExpressionBuilder,
                                                   QueryRequest queryRequest, List<Sort> sorts) {
        String hashKey = conditionExpressionBuilder.getHashKey();
        String rangeKey = conditionExpressionBuilder.getRangeKey();
        IndexData indexData;

        if (sorts == null || sorts.isEmpty()) {
            indexData = indexes.get(hashKey).get(rangeKey);
        } else if (sorts.size() == 1) {
            Sort sort = sorts.get(0);
            String sortKey = sort.getField();

            // DynamoDB can only sort on the effective range key.  If a range key is specified, ensure the range key and
            // sort key are the same.
            if (rangeKey != null && !rangeKey.equals(sortKey)) {
                throw new JeppettoException("DynamoDB can only sort on the effective range key. Unable to sort on: " + sortKey);
            }

            queryRequest.setScanIndexForward(sort.getSortDirection() == SortDirection.Ascending);

            // Index is based off the sort key
            indexData = indexes.get(hashKey).get(sortKey);
        } else {
            throw new JeppettoException("DynamoDB only supports one sort value.");
        }

        if (indexData.indexName != null && !indexData.projectsOverEntity) {
            logger.warn("Query using index {} incurs additional costs to fully fetch a {} type. Use a projected object"
                        + " DAO to avoid this overhead.", indexData.indexName, entityClass.getSimpleName());
        }

        queryRequest.setIndexName(indexData.indexName);

        return indexData.keyFields;
    }


    private void applyExpressions(ConditionExpressionBuilder conditionExpressionBuilder, QueryRequest queryRequest) {
        Map<String, String> expressionAttributeNames;

        queryRequest.setProjectionExpression(projectionExpression);

        if (conditionExpressionBuilder.hasExpression()) {
            queryRequest.setFilterExpression(conditionExpressionBuilder.getExpression());

            if (!conditionExpressionBuilder.getExpressionAttributeValues().isEmpty()) {
                queryRequest.setExpressionAttributeValues(conditionExpressionBuilder.getExpressionAttributeValues());
            }

            if (projectionExpressionNames.isEmpty()) {
                expressionAttributeNames = conditionExpressionBuilder.getExpressionAttributeNames();
            } else if (conditionExpressionBuilder.getExpressionAttributeNames().isEmpty()) {
                expressionAttributeNames = projectionExpressionNames;
            } else {
                expressionAttributeNames = new LinkedHashMap<String, String>();
                expressionAttributeNames.putAll(conditionExpressionBuilder.getExpressionAttributeNames());
                expressionAttributeNames.putAll(projectionExpressionNames);
            }
        } else {
            expressionAttributeNames = projectionExpressionNames;
        }

        if (!expressionAttributeNames.isEmpty()) {
            queryRequest.setExpressionAttributeNames(expressionAttributeNames);
        }
    }


    private Iterable<T> scanItems(QueryModel queryModel, ConditionExpressionBuilder conditionExpressionBuilder) {
        ScanRequest scanRequest = new ScanRequest(tableName);

        if (queryModel.getFirstResult() > 0) {
            logger.warn("DynamoDB does not support skipping results.  Call setPosition() on DynamoDBIterable instead.");
        }

        if (queryModel.getMaxResults() > 0) {
            scanRequest.setLimit(queryModel.getMaxResults());
        }

        if (queryModel.getSorts() != null) {
            logger.warn("Not able to sort when performing a 'scan' operation.  Ignoring... ");
        }

        Map<String, String> expressionAttributeNames;

        scanRequest.setProjectionExpression(projectionExpression);

        if (conditionExpressionBuilder.hasExpression()) {
            scanRequest.setFilterExpression(conditionExpressionBuilder.getExpression());

            if (!conditionExpressionBuilder.getExpressionAttributeValues().isEmpty()) {
                scanRequest.setExpressionAttributeValues(conditionExpressionBuilder.getExpressionAttributeValues());
            }

            if (projectionExpressionNames.isEmpty()) {
                expressionAttributeNames = conditionExpressionBuilder.getExpressionAttributeNames();
            } else if (conditionExpressionBuilder.getExpressionAttributeNames().isEmpty()) {
                expressionAttributeNames = projectionExpressionNames;
            } else {
                expressionAttributeNames = new LinkedHashMap<String, String>();
                expressionAttributeNames.putAll(conditionExpressionBuilder.getExpressionAttributeNames());
                expressionAttributeNames.putAll(projectionExpressionNames);
            }
        } else {
            expressionAttributeNames = projectionExpressionNames;
        }

        if (!expressionAttributeNames.isEmpty()) {
            scanRequest.setExpressionAttributeNames(expressionAttributeNames);
        }

        return new ScanIterable<T>(dynamoDB, persistableEnhancer, scanRequest,
                                   rangeKeyField == null ? Collections.singleton(hashKeyField)
                                                         : Arrays.asList(hashKeyField, rangeKeyField));
    }


    private <U extends T> ResultFromUpdate getResultFromUpdate(U updateObject) {
        if (UpdateBehaviorDescriptor.class.isAssignableFrom(updateObject.getClass())) {
            ResultFromUpdate resultFromUpdate = ((UpdateBehaviorDescriptor) updateObject).getResultFromUpdate();

            return resultFromUpdate != null ? resultFromUpdate : ResultFromUpdate.ReturnNone;
        } else {
            return ResultFromUpdate.ReturnNone;
        }
    }


    private void generateIdIfNeeded(DynamoDBPersistable dynamoDBPersistable) {
        if (dynamoDBPersistable.__get(hashKeyField) != null
         /* && rangeKeyField != null && dynamoDBPersistable.__get(rangeKeyField) != null */) {
            return;
        }

        if (idGenerator == null) {
            throw new JeppettoException("No id provided, and no id generator available.");
        }

        // TODO: handle case when part of the key is there (e.g. code generates range key, but wants to generate hash key)
        // Can't blindly use getKeyFrom since a single generated value may be for the range key...
        dynamoDBPersistable.__putAll(getKeyFrom(idGenerator.generateId()));
    }


    private AttributeValue getAttributeValue(Object value) {
        if (Number.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue().withN(value.toString());
        } else {
            return new AttributeValue(value.toString());
        }
    }


    private Pair<String, String> getKeyAttributeNames(List<KeySchemaElement> keySchema) {
        Pair<String, String> keyAttributes = new Pair<String, String>();

        for (KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.getKeyType().equals(KeyType.HASH.name())) {
                keyAttributes.setFirst(keySchemaElement.getAttributeName());
            } else {
                keyAttributes.setSecond(keySchemaElement.getAttributeName());
            }
        }

        return keyAttributes;
    }


    private Map<String, AttributeValue> getKeyFrom(ID id) {
        Map<String, AttributeValue> key;

        if (Pair.class.isAssignableFrom(id.getClass())) {
            key = new HashMap<String, AttributeValue>(2);

            key.put(hashKeyField, getAttributeValue(((Pair) id).getFirst()));
            key.put(rangeKeyField, getAttributeValue(((Pair) id).getSecond()));
        } else {
            key = Collections.singletonMap(hashKeyField, getAttributeValue(id));
        }

        return key;
    }


    private Map<String, AttributeValue> getKeyFrom(DynamoDBPersistable dynamoDBPersistable) {
        Map<String, AttributeValue> key;

        if (rangeKeyField != null) {
            key = new HashMap<String, AttributeValue>(2);

            key.put(hashKeyField, ConversionUtil.toAttributeValue(dynamoDBPersistable.__get(hashKeyField)));
            key.put(rangeKeyField, ConversionUtil.toAttributeValue(dynamoDBPersistable.__get(rangeKeyField)));
        } else {
            key = Collections.singletonMap(hashKeyField, ConversionUtil.toAttributeValue(dynamoDBPersistable.__get(
                    hashKeyField)));
        }

        return key;
    }


    //-------------------------------------------------------------
    // Inner Classes
    //-------------------------------------------------------------

    public static class IndexData {
        public String indexName;
        public List<String> keyFields;
        public boolean projectsOverEntity;

        private IndexData(String indexName, List<String> keyFields, boolean projectsOverEntity) {
            this.indexName = indexName;
            this.keyFields = keyFields;
            this.projectsOverEntity = projectsOverEntity;
        }
    }
}
