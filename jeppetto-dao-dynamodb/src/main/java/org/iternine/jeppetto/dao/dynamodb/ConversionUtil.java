/*
 * Copyright (c) 2011-2017 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.dynamodb;


import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.persistable.PersistableList;
import org.iternine.jeppetto.dao.persistable.PersistableMap;
import org.iternine.jeppetto.enhance.Enhancer;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ConversionUtil {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Set<Class> STORE_AS_NUMBER_CLASSES = new HashSet<Class>() {{
        add(Date.class);
        add(byte.class);
        add(short.class);
        add(int.class);
        add(long.class);
        add(float.class);
        add(double.class);
        add(Number.class);
        add(BigDecimal.class);
        add(BigInteger.class);
        add(Byte.class);
        add(Short.class);
        add(Integer.class);
        add(Long.class);
        add(Float.class);
        add(Double.class);
    }};


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public static Map<String, AttributeValue> getItemFromObject(final DynamoDBPersistable dynamoDBPersistable) {
        Map<String, AttributeValue> itemMap = new HashMap<String, AttributeValue>();

        for (Iterator<String> dirtyFields = dynamoDBPersistable.__getDirtyFields(); dirtyFields.hasNext(); ) {
            String dirtyField = dirtyFields.next();
            Object object = dynamoDBPersistable.__get(dirtyField);

            // We don't save nulls.  If it turns out we need it, we could have a "saveNulls" configuration value.
            // If saveNulls and attributeValue == null: itemMap.put(dirtyField, new AttributeValue().withNULL(Boolean.TRUE));
            // Note we might want something like a thread local 'PersistenceContext' with saveNulls or have a
            // DynamoDBPersistable.saveNulls(saveNulls) method
            if (object != null) {
                itemMap.put(dirtyField, toAttributeValue(object));
            }
        }

        return itemMap;
    }


    public static <T> AttributeValue toAttributeValue(final T value) {
        return toAttributeValue(value, null);
    }


    /*
     * In the future, we may want to support storing associated objects w/in this value in different
     * DynamoDB tables.  In that case, as this method was navigating the value and came upon a ...
     * TODO: finish comment
     */
    @SuppressWarnings("unchecked")
    public static <T> AttributeValue toAttributeValue(final T value, final Class collectionType) {
        if (value == null) {
            return null;
        } else if (String.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue((String) value);
        } else if (Number.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue().withN(value.toString());
        } else if (Boolean.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue().withBOOL((Boolean) value);
        } else if (Character.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue(value.toString());
        } else if (Byte.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue().withN(value.toString());
        } else if (byte[].class.isAssignableFrom(value.getClass())) {
            return new AttributeValue().withB(ByteBuffer.wrap((byte[]) value));
        } else if (Date.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue().withN(Long.toString(((Date) value).getTime()));
        } else if (Enum.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue(((Enum) value).name());
        } else if (Set.class.isAssignableFrom(value.getClass())) {
            Set valueSet = (Set) value;
            Set<String> strings = new HashSet<String>(valueSet.size());

            for (Object next : valueSet) {
                strings.add(toString(next));
            }

            if (collectionType != null && STORE_AS_NUMBER_CLASSES.contains(collectionType)) {
                return new AttributeValue().withNS(strings);
            } else {
                return new AttributeValue().withSS(strings);
            }
        } else if (List.class.isAssignableFrom(value.getClass())) {
            List valueList = (List) value;
            List<AttributeValue> attributeValues = new ArrayList<AttributeValue>(valueList.size());

            for (Object next : valueList) {
                attributeValues.add(toAttributeValue(next));
            }

            return new AttributeValue().withL(attributeValues);
        } else if (Map.class.isAssignableFrom(value.getClass())) {
            Map<String, ?> valueMap = (Map<String, Object>) value;
            Map<String, AttributeValue> attributeValueMap = new HashMap<String, AttributeValue>(valueMap.size());

            for (Map.Entry<String, ?> entry : valueMap.entrySet()) {
                attributeValueMap.put(entry.getKey(), toAttributeValue(entry.getValue()));
            }

            return new AttributeValue().withM(attributeValueMap);
        } else if (AttributeValue.class.isAssignableFrom(value.getClass())) {
            return (AttributeValue) value;
        } else {
            Enhancer<T> enhancer = EnhancerHelper.getPersistableEnhancer((Class<T>) value.getClass());
            DynamoDBPersistable dynamoDBPersistable = (DynamoDBPersistable) enhancer.enhance(value);

            return new AttributeValue().withM(getItemFromObject(dynamoDBPersistable));
        }
    }


    public static <T> T getObjectFromItem(final Map<String, AttributeValue> item, final Class<T> targetType) {
        T t = EnhancerHelper.getPersistableEnhancer(targetType).newInstance();

        ((DynamoDBPersistable) t).__putAll(item);

        return t;
    }


    public static Object fromAttributeValue(final AttributeValue attributeValue, final Class targetType,
                                            final Class collectionType) {
        if (String.class.isAssignableFrom(targetType)) {
            return attributeValue.getS();
        } else if (Integer.class.isAssignableFrom(targetType) || int.class.isAssignableFrom(targetType)) {
            return Integer.valueOf(attributeValue.getN());
        } else if (Long.class.isAssignableFrom(targetType) || long.class.isAssignableFrom(targetType)) {
            return Long.valueOf(attributeValue.getN());
        } else if (Double.class.isAssignableFrom(targetType) || double.class.isAssignableFrom(targetType)) {
            return Double.valueOf(attributeValue.getN());
        } else if (Float.class.isAssignableFrom(targetType) || float.class.isAssignableFrom(targetType)) {
            return Float.valueOf(attributeValue.getN());
        } else if (Boolean.class.isAssignableFrom(targetType) || boolean.class.isAssignableFrom(targetType)) {
            return attributeValue.getBOOL();
        } else if (Character.class.isAssignableFrom(targetType) || char.class.isAssignableFrom(targetType)) {
            return attributeValue.getS().charAt(0);
        } else if (Byte.class.isAssignableFrom(targetType) || byte.class.isAssignableFrom(targetType)) {
            return Byte.valueOf(attributeValue.getN());
        } else if (Byte[].class.isAssignableFrom(targetType) || byte[].class.isAssignableFrom(targetType)) {
            return attributeValue.getB().array();
        } else if (Short.class.isAssignableFrom(targetType) || short.class.isAssignableFrom(targetType)) {
            return Short.valueOf(attributeValue.getN());
        } else if (Date.class.isAssignableFrom(targetType)) {
            return new Date(Long.parseLong(attributeValue.getN()));
        } else if (Enum.class.isAssignableFrom(targetType)) {
            //noinspection unchecked
            return Enum.valueOf((Class<Enum>) targetType, attributeValue.getS());
        } else if (Set.class.isAssignableFrom(targetType)) {
            Set result = new HashSet();

            List<String> strings;
            if (attributeValue.getSS() != null) {
                strings = attributeValue.getSS();
            } else {
                strings = attributeValue.getNS();
            }

            for (String string : strings) {
                //noinspection unchecked
                result.add(fromString(string, collectionType));
            }

            return result;
        } else if (List.class.isAssignableFrom(targetType)) {
            List<AttributeValue> attributeValues = attributeValue.getL();
            List result = new PersistableList(attributeValues.size());

            for (AttributeValue value : attributeValues) {
                // NB: We currently only pick up the first type level for an attribute value.  So if, for example,
                // someone had a List<List<Integer>>, we would not be able to rebuild it.  The solution is
                // TODO: change the way this works to handle nested types?

                //noinspection unchecked
                result.add(fromAttributeValue(value, collectionType, null));
            }

            return result;
        } else if (Map.class.isAssignableFrom(targetType)) {
            Map<String, AttributeValue> attributeValues = attributeValue.getM();
            Map result = new PersistableMap(attributeValues.size());

            for (Map.Entry<String, AttributeValue> entry : attributeValues.entrySet()) {
                //noinspection unchecked
                result.put(entry.getKey(), fromAttributeValue(entry.getValue(), collectionType, null));
            }

            return result;
        } else {
            return getObjectFromItem(attributeValue.getM(), targetType);
        }
    }


    public static Collection<AttributeValue> toAttributeValueList(Object value) {
        Collection<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();

        if (value.getClass().isArray()) {
            int length = Array.getLength(value);

            for (int i = 0; i < length; i ++) {
                attributeValueList.add(toAttributeValue(Array.get(value, i)));
            }
        } else if (Collection.class.isAssignableFrom(value.getClass())) {
            for (Object item : (Collection) value) {
                attributeValueList.add(toAttributeValue(item));
            }
        } else {
            throw new JeppettoException("Expected either array or Collection object.");
        }

        return attributeValueList;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private static String toString(Object value) {
        if (Date.class.isAssignableFrom(value.getClass())) {
            return Long.toString(((Date) value).getTime());
        } else if (Enum.class.isAssignableFrom(value.getClass())) {
            return ((Enum) value).name();
        }

        return value.toString();
    }


    private static Object fromString(String string, Class type) {
        if (type == null || String.class.isAssignableFrom(type)) {
            return string;
        } else if (Integer.class.isAssignableFrom(type) || int.class.isAssignableFrom(type)) {
            return Integer.valueOf(string);
        } else if (Long.class.isAssignableFrom(type) || long.class.isAssignableFrom(type)) {
            return Long.valueOf(string);
        } else if (Double.class.isAssignableFrom(type) || double.class.isAssignableFrom(type)) {
            return Double.valueOf(string);
        } else if (Float.class.isAssignableFrom(type) || float.class.isAssignableFrom(type)) {
            return Float.valueOf(string);
        } else if (Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type)) {
            return Boolean.valueOf(string);
        } else if (Character.class.isAssignableFrom(type) || char.class.isAssignableFrom(type)) {
            return string.charAt(0);
        } else if (Byte.class.isAssignableFrom(type) || byte.class.isAssignableFrom(type)) {
            return Byte.valueOf(string);
        } else if (Short.class.isAssignableFrom(type) || short.class.isAssignableFrom(type)) {
            return Short.valueOf(string);
        } else if (Date.class.isAssignableFrom(type)) {
            return new Date(Long.parseLong(string));
        } else if (Enum.class.isAssignableFrom(type)) {
            //noinspection unchecked
            return Enum.valueOf((Class<Enum>) type, string);
        }

        throw new RuntimeException("Unhandled type: " + type);
    }
}
