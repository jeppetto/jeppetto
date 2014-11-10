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
import org.iternine.jeppetto.dao.FailedBatchDeleteException;
import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.OptimisticLockException;
import org.iternine.jeppetto.dao.Pair;
import org.iternine.jeppetto.dao.Projection;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.QueryModel;
import org.iternine.jeppetto.dao.QueryModelDAO;
import org.iternine.jeppetto.dao.ReferenceSet;
import org.iternine.jeppetto.dao.Sort;
import org.iternine.jeppetto.dao.SortDirection;
import org.iternine.jeppetto.dao.TooManyItemsException;
import org.iternine.jeppetto.dao.dynamodb.iterable.BatchGetIterable;
import org.iternine.jeppetto.dao.dynamodb.iterable.QueryIterable;
import org.iternine.jeppetto.dao.dynamodb.iterable.ScanIterable;
import org.iternine.jeppetto.dao.id.IdGenerator;
import org.iternine.jeppetto.enhance.Enhancer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


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

    private static final Set<ComparisonOperator> RANGE_KEY_COMPARISON_OPERATORS = new HashSet<ComparisonOperator>() {{
        add(ComparisonOperator.EQ);
        add(ComparisonOperator.LE);
        add(ComparisonOperator.LT);
        add(ComparisonOperator.GE);
        add(ComparisonOperator.GT);
        add(ComparisonOperator.BEGINS_WITH);
        add(ComparisonOperator.BETWEEN);
    }};

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBQueryModelDAO.class);


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final Class<T> entityClass;
    private final AmazonDynamoDB dynamoDB;
    private final String tableName;
    private final IdGenerator<ID> idGenerator;
    private final boolean consistentRead;

    private final String hashKeyField;
    private final String rangeKeyField;
    private final Map<String, String> localIndexes;
//    private Map<Pair<String, String>, String> globalIndexes;
    private final Enhancer<T> enhancer;


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
        this.consistentRead = Boolean.parseBoolean((String) daoProperties.get("consistentRead"));   // null okay

        TableDescription tableDescription = dynamoDB.describeTable(tableName).getTable();

        Pair<String, String> primaryKeyAttributeNames = getKeyAttributeNames(tableDescription.getKeySchema());
        this.hashKeyField = primaryKeyAttributeNames.getFirst();
        this.rangeKeyField = primaryKeyAttributeNames.getSecond();

        List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription.getLocalSecondaryIndexes();
        if (localSecondaryIndexes != null) {
            this.localIndexes = new HashMap<String, String>(localSecondaryIndexes.size() + 1);

            for (LocalSecondaryIndexDescription description : localSecondaryIndexes) {
                localIndexes.put(getKeyAttributeNames(description.getKeySchema()).getSecond(), description.getIndexName());
            }

            // We include the primary range key as an local index (w/o an index name) to make findUsingQueryModel() code below simpler
            localIndexes.put(rangeKeyField, null);
        } else {
            this.localIndexes = Collections.singletonMap(rangeKeyField, null);
        }

        // TODO: Handle GSIs
//        List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = tableDescription.getGlobalSecondaryIndexes();
//        if (!globalSecondaryIndexes.isEmpty()) {
//            globalIndexes = new HashMap<Pair<String, String>, String>(globalSecondaryIndexes.size());
//
//            for (GlobalSecondaryIndexDescription description : globalSecondaryIndexes) {
//                globalIndexes.put(getKeyAttributeNames(description.getKeySchema()), description.getIndexName());
//            }
//        }

        this.enhancer = EnhancerHelper.getDynamoDBPersistableEnhancer(entityClass);
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(ID id)
            throws NoSuchItemException, JeppettoException {
        GetItemResult result;

        try {
            result = dynamoDB.getItem(new GetItemRequest(tableName, getKeyFromId(id)));
        } catch (AmazonClientException e) {
            throw new JeppettoException(e);
        }

        if (result.getItem() == null) {
            throw new NoSuchItemException(entityClass.getSimpleName(), id.toString());
        }

        T t = ConversionUtil.getObjectFromItem(result.getItem(), entityClass);

        ((DynamoDBPersistable) t).markPersisted(dynamoDB.toString());

        return t;
    }


    @Override
    public Iterable<T> findByIds(ID... ids)
            throws JeppettoException {
        Collection<Map<String, AttributeValue>> keys = new ArrayList<Map<String, AttributeValue>>();

        for (ID id : ids) {
            keys.add(getKeyFromId(id));
        }

        Map<String, KeysAndAttributes> requestItems = Collections.singletonMap(tableName, new KeysAndAttributes().withKeys(keys));
        BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest().withRequestItems(requestItems);

        return new BatchGetIterable<T>(dynamoDB, enhancer, batchGetItemRequest, tableName);
    }


    @Override
    public Iterable<T> findAll()
            throws JeppettoException {
        return findUsingQueryModel(new QueryModel());
    }


    @Override
    public void save(T entity)
            throws OptimisticLockException, JeppettoException {
        DynamoDBPersistable dynamoDBPersistable = (DynamoDBPersistable) enhancer.enhance(entity);

        if (!dynamoDBPersistable.isPersisted(dynamoDB.toString())) {
            saveItem(dynamoDBPersistable);
        } else {
            updateItem(dynamoDBPersistable);
        }

        dynamoDBPersistable.markPersisted(dynamoDB.toString());
    }


    @Override
    public void delete(T entity)
            throws JeppettoException {
        if (entity == null) {
            throw new JeppettoException("entity is null; nothing to delete.");
        }

        deleteItem(getKey((DynamoDBPersistable) enhancer.enhance(entity)));
    }


    @Override
    public void deleteById(ID id)
            throws JeppettoException {
        if (id == null) {
            throw new JeppettoException("id is null; unable to delete entity.");
        }

        deleteItem(getKeyFromId(id));
    }


    @Override
    public void deleteByIds(ID... ids)
            throws FailedBatchDeleteException, JeppettoException {
        List<ID> failedDeletes = new ArrayList<ID>();

        for (ID id : ids) {
            try {
                deleteItem(getKeyFromId(id));
            } catch (Exception e) {
                logger.warn("Failed to delete item with id = " + id, e);

                failedDeletes.add(id);
            }
        }

        if (failedDeletes.size() > 0) {
            throw new FailedBatchDeleteException(failedDeletes);
        }
    }


    @Override
    public ReferenceSet<T> referenceByIds(ID... ids) {
        throw new UnsupportedOperationException("referenceByIds() not yet supported.");
    }


    @Override
    public void updateReferences(ReferenceSet<T> referenceSet, T updateObject)
            throws JeppettoException {
        throw new UnsupportedOperationException("updateReferences() not yet supported.");
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
        queryModel.setMaxResults(2);

        Iterator<T> results = findUsingQueryModel(queryModel).iterator();

        if (!results.hasNext()) {
            throw new NoSuchItemException();
        }

        T result = results.next();

        if (results.hasNext()) {
            throw new TooManyItemsException();
        }

        return result;
    }


    @Override
    public Iterable<T> findUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        Condition hashKeyCondition = null;
        Condition actingRangeKeyCondition = null;
        ConditionExpressionBuilder conditionExpressionBuilder = new ConditionExpressionBuilder();

        if (queryModel.getConditions() != null) {
            for (Condition condition : queryModel.getConditions()) {
                DynamoDBConstraint dynamoDBConstraint = (DynamoDBConstraint) condition.getConstraint();
                ComparisonOperator comparisonOperator = dynamoDBConstraint.getOperator().getComparisonOperator();

                if (condition.getField().equals(hashKeyField) && comparisonOperator == ComparisonOperator.EQ) {
                    hashKeyCondition = condition;
                } else if (actingRangeKeyCondition == null     // First one wins...
                           && localIndexes.containsKey(condition.getField())
                           && RANGE_KEY_COMPARISON_OPERATORS.contains(comparisonOperator)) {
                    actingRangeKeyCondition = condition;
                } else {
                    conditionExpressionBuilder.add(condition.getField(), dynamoDBConstraint);
                }
            }
        }

        if (queryModel.getAssociationConditions() != null) {
            for (Map.Entry<String, List<Condition>> associationConditions : queryModel.getAssociationConditions().entrySet()) {
                for (Condition condition : associationConditions.getValue()) {
                    conditionExpressionBuilder.add(associationConditions.getKey() + "." + condition.getField(),
                                                   (DynamoDBConstraint) condition.getConstraint());
                }
            }
        }

        if (hashKeyCondition != null) {
            Map<String, com.amazonaws.services.dynamodbv2.model.Condition> keyConditions
                    = new HashMap<String, com.amazonaws.services.dynamodbv2.model.Condition>();
            String actingRangeKey;

            keyConditions.put(hashKeyCondition.getField(), ((DynamoDBConstraint) hashKeyCondition.getConstraint()).asCondition());

            if (actingRangeKeyCondition == null) {
                actingRangeKey = null;
            } else {
                actingRangeKey = actingRangeKeyCondition.getField();
                keyConditions.put(actingRangeKey, ((DynamoDBConstraint) actingRangeKeyCondition.getConstraint()).asCondition());
            }

            return queryItems(queryModel, keyConditions, actingRangeKey, conditionExpressionBuilder);
        } else {
            if (actingRangeKeyCondition != null) {
                conditionExpressionBuilder.add(actingRangeKeyCondition.getField(), (DynamoDBConstraint) actingRangeKeyCondition.getConstraint());
            }

            return scanItems(queryModel, conditionExpressionBuilder);
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

        for (T match : matches) {
            delete(match);
        }
    }


    @Override
    public ReferenceSet<T> referenceUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        throw new UnsupportedOperationException("referenceUsingQueryModel() not yet supported.");
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

    private void saveItem(DynamoDBPersistable dynamoDBPersistable) {
        generateIdIfNeeded(dynamoDBPersistable);

        try {
            dynamoDB.putItem(new PutItemRequest(tableName, ConversionUtil.getItemFromObject(dynamoDBPersistable)));
        } catch (Exception e) {
            throw new JeppettoException(e);
        }
    }


    private void updateItem(DynamoDBPersistable dynamoDBPersistable) {
        UpdateExpressionBuilder updateExpressionBuilder = new UpdateExpressionBuilder(dynamoDBPersistable);

        try {
            dynamoDB.updateItem(new UpdateItemRequest().withTableName(tableName)
                                                       .withKey(getKey(dynamoDBPersistable))
                                                       .withUpdateExpression(updateExpressionBuilder.getExpression())
                                                       .withExpressionAttributeValues(updateExpressionBuilder.getAttributeValues()));
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


    private Iterable<T> queryItems(QueryModel queryModel, Map<String, com.amazonaws.services.dynamodbv2.model.Condition> keyConditions,
                                   String actingRangeKey, ConditionExpressionBuilder conditionExpressionBuilder) {
        QueryRequest queryRequest = new QueryRequest(tableName);

        queryRequest.setKeyConditions(keyConditions);
        queryRequest.setIndexName(localIndexes.get(actingRangeKey));
        queryRequest.setConsistentRead(consistentRead);

        if (queryModel.getFirstResult() > 0) {
            logger.warn("DynamoDB does not support skipping results.  Call setPosition() on DynamoDBIterable instead.");
        }

        if (queryModel.getMaxResults() > 0) {
            queryRequest.setLimit(queryModel.getMaxResults());
        }

        if (queryModel.getSorts() != null) {
            for (Sort sort : queryModel.getSorts()) {
                if (!sort.getField().equals(actingRangeKey)) {
                    logger.warn("Unable to sort on other than the acting range key. Ignoring sort on " + sort.getField());
                }

                queryRequest.setScanIndexForward(sort.getSortDirection() == SortDirection.Ascending);
            }
        }

        if (conditionExpressionBuilder.hasConditions()) {
            queryRequest.setFilterExpression(conditionExpressionBuilder.getExpression());
            queryRequest.setExpressionAttributeValues(conditionExpressionBuilder.getAttributeValues());
        }

        // TODO: if partial object, add attributes to fetch
//            queryRequest.setAttributesToGet();

        return new QueryIterable<T>(dynamoDB, enhancer, queryRequest, hashKeyField);
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

        if (conditionExpressionBuilder.hasConditions()) {
            scanRequest.setFilterExpression(conditionExpressionBuilder.getExpression());
            scanRequest.setExpressionAttributeValues(conditionExpressionBuilder.getAttributeValues());
        }

        // TODO: if partial object, add attributes to fetch
//            scanRequest.setAttributesToGet();

        return new ScanIterable<T>(dynamoDB, enhancer, scanRequest, hashKeyField);
    }


    private void generateIdIfNeeded(DynamoDBPersistable dynamoDBPersistable) {
        if (dynamoDBPersistable.get(hashKeyField) != null
         /* && rangeKeyField != null && dynamoDBPersistable.get(rangeKeyField) != null */) {
            return;
        }

        // TODO: handle case when part of the key is there (e.g. code generates range key, but wants to generate hash key)
        // Can't blindly use getKeyFromId since a single generated value may be for the range key...
        dynamoDBPersistable.putAll(getKeyFromId(idGenerator.generateId()));
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


    private Map<String, AttributeValue> getKeyFromId(ID id) {
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


    public Map<String, AttributeValue> getKey(DynamoDBPersistable dynamoDBPersistable) {
        Map<String, AttributeValue> key;

        if (rangeKeyField != null) {
            key = new HashMap<String, AttributeValue>(2);

            key.put(hashKeyField, ConversionUtil.toAttributeValue(dynamoDBPersistable.get(hashKeyField)));
            key.put(rangeKeyField, ConversionUtil.toAttributeValue(dynamoDBPersistable.get(rangeKeyField)));
        } else {
            key = Collections.singletonMap(hashKeyField, ConversionUtil.toAttributeValue(
                    dynamoDBPersistable.get(hashKeyField)));
        }

        return key;
    }
}
