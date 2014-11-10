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


import org.iternine.jeppetto.dao.dirtyable.Dirtyable;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;


public interface DynamoDBPersistable extends Dirtyable {

    /**
     * @param key of the field to retrieve
     *
     * @return the associated field's value
     */
    Object get(String key);


    /**
     * @param key of the field to update
     * @param value to place in the associated field
     */
    void put(String key, AttributeValue value);


    /**
     * @param itemMap that contains key, value pairs to apply to this object
     */
    void putAll(Map<String, AttributeValue> itemMap);
}
