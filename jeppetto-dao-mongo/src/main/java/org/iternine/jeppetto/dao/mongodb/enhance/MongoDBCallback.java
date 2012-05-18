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

package org.iternine.jeppetto.dao.mongodb.enhance;


import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBCallback;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class MongoDBCallback extends DefaultDBCallback {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private static Map<String, Class> classCache = new HashMap<String, Class>();
    private DBCollection dbCollection;

    private static Logger logger = LoggerFactory.getLogger(MongoDBCallback.class);

    // List of nested documents that come back from an "explain()" call.  This callback is used
    // to construct the result object, but these fields do not have a corresponding sub-object.
    private static final List<String> EXPLAIN_PATHS_TO_IGNORE = Arrays.asList("allPlans",
                                                                              "indexBounds",
                                                                              "shards");


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public MongoDBCallback(DBCollection dbCollection) {
        super(dbCollection);

        if (dbCollection != null && !dbCollection.getName().equals("$cmd")) {
            this.dbCollection = dbCollection;
        }
    }


    //-------------------------------------------------------------
    // Methods - Overrides - DBCallback
    //-------------------------------------------------------------

    @Override
    public BSONObject create(boolean array, List<String> pathParts) {
        if (pathParts == null || dbCollection == null) {
            return super.create(array, pathParts);
        }

        String path = buildPath(pathParts);
        Class returnClass;

        if ((returnClass = getClassFromCache(path)) == null) {
            returnClass = deriveClass(path, pathParts.get(pathParts.size() - 1), array);
        }

        // At this point, we know what class to construct and the class cache is properly set

        if (DBObject.class.isAssignableFrom(returnClass)) {
            try {
                return (DBObject) returnClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (Map.class.isAssignableFrom(returnClass)) {
            if (Modifier.isAbstract(returnClass.getModifiers()) || Modifier.isInterface(returnClass.getModifiers())) {
                return new DirtyableDBObjectMap();
            } else {
                try {
                    return new DirtyableDBObjectMap((Map) returnClass.newInstance());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (List.class.isAssignableFrom(returnClass)) {
            if (Modifier.isAbstract(returnClass.getModifiers()) || Modifier.isInterface(returnClass.getModifiers())) {
                return new DirtyableDBObjectList();
            } else {
                try {
                    return new DirtyableDBObjectList((List) returnClass.newInstance());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (Set.class.isAssignableFrom(returnClass)) {
            if (Modifier.isAbstract(returnClass.getModifiers()) || Modifier.isInterface(returnClass.getModifiers())) {
                return new DirtyableDBObjectSet();
            } else {
                try {
                    return new DirtyableDBObjectSet((Set) returnClass.newInstance());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            return new BasicDBObject();
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private String buildPath(List<String> path) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < path.size(); i++) {
            if (i > 0) {
                sb.append('.');
            }

            sb.append(path.get(i));
        }

        return sb.toString();
    }


    private Class getClassFromCache(String path) {
        Class clazz = classCache.get(dbCollection.getName() + "." + path);

        if (clazz != null) {
            return clazz;
        }

        int lastDotIndex = path.lastIndexOf('.');

        if (lastDotIndex > 0) {
            return classCache.get(dbCollection.getName() + "." + path.substring(0, lastDotIndex + 1));
        }

        return null;
    }


    /* tests:
     *   names at different depths
     *   maps w/ other objects as keys
     *
     * ""
     * relatedMongoObjectMap                                    relatedMongoObjectMap       Map<>
     * relatedMongoObjectMap.foo                                relatedMongoObjectMap.      RelatedObject
     * nestedSimpleMongoObject                                  nestedSimpleMongoObject     SimpleObject
     * nestedSimpleMongoObject.relatedMongoObjectMap            nestedSimpleMongoObject.relatedObjectMap    Map<>
     * nestedSimpleMongoObject.relatedMongoObjectMap.bar        nestedSimpleMongoObject.relatedObjectMap.   RelatedObject
     */
    private Class deriveClass(String path, String lastPathPart, boolean array) {
        Class containerClass;

        if (path.equals(lastPathPart)) {
            containerClass = dbCollection.getObjectClass();
        } else {
            containerClass = classCache.get(dbCollection.getName() + "." + path.substring(0, path.lastIndexOf('.')));
        }

        if (containerClass != null && DBObject.class.isAssignableFrom(containerClass)) {
            try {
                Method m = containerClass.getMethod("__getPreEnhancedClass");

                containerClass = (Class) m.invoke(null);
            } catch (Exception e) {
                logger.warn("DBObject without __getPreEnhancedClass() method.  Was the container class enhanced?");
            }
        }

        // If we don't have a container class at this point, we are in a part of the result document that
        // does not correspond to the object model.  Return a basic MongoDB object.
        if (containerClass == null) {
            return array ? BasicDBList.class : BasicDBObject.class;
        }

        Method getter;

        try {
            // noinspection ConstantConditions
            getter = containerClass.getMethod("get" + Character.toUpperCase(lastPathPart.charAt(0)) + lastPathPart.substring(1));
        } catch (NoSuchMethodException e) {
            if (!EXPLAIN_PATHS_TO_IGNORE.contains(lastPathPart) && !lastPathPart.startsWith("__")) {
                logger.warn("No getter for: {} ({})", lastPathPart, e.getMessage());
            }

            return array ? BasicDBList.class : BasicDBObject.class;
        }

        Type returnType = getter.getGenericReturnType();
        String qualifiedPath = dbCollection.getName() + "." + path;

        if (Class.class.isAssignableFrom(returnType.getClass())) {
            // noinspection unchecked
            Class enhancedClass = EnhancerHelper.getDirtyableDBObjectEnhancer((Class) returnType).getEnhancedClass();

            classCache.put(qualifiedPath, enhancedClass);

            return enhancedClass;
        } else if (ParameterizedType.class.isAssignableFrom(returnType.getClass())) {
            ParameterizedType parameterizedType = (ParameterizedType) returnType;
            Class rawType = (Class) parameterizedType.getRawType();
            Class rawClass;
            Class enhancedClass;

            if (Map.class.isAssignableFrom(rawType)) {
                rawClass = (Class) parameterizedType.getActualTypeArguments()[1];
            } else if (Iterable.class.isAssignableFrom(rawType)) {
                rawClass = (Class) parameterizedType.getActualTypeArguments()[0];
            } else {
                throw new RuntimeException("unknown type: " + rawType);
            }

            classCache.put(qualifiedPath, rawType);

            if (!DBObjectUtil.needsNoConversion(rawClass)) {
                //noinspection unchecked
                enhancedClass = EnhancerHelper.getDirtyableDBObjectEnhancer(rawClass).getEnhancedClass();

                classCache.put(qualifiedPath + ".", enhancedClass);
            }

            return rawType;
        } else {
            throw new RuntimeException("Don't know how to handle: " + qualifiedPath);
        }
    }
}
