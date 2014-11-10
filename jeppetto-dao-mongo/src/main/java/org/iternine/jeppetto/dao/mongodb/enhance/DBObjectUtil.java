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


import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.enhance.Enhancer;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRefBase;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


public class DBObjectUtil {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Set<Class<?>> NO_CONVERSION_CLASSES = new HashSet<Class<?>>();


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    static {
        //noinspection unchecked
        Collections.addAll(NO_CONVERSION_CLASSES,
                           Date.class,
                           boolean.class,
                           char.class,
                           byte.class,
                           short.class,
                           int.class,
                           long.class,
                           float.class,
                           double.class,
                           Number.class,
                           BigDecimal.class,
                           BigInteger.class,
                           String.class,
                           Boolean.class,
                           Character.class,
                           Byte.class,
                           Short.class,
                           Integer.class,
                           Long.class,
                           Float.class,
                           Double.class,
                           Pattern.class,
                           byte[].class,
                           DBRefBase.class,
                           ObjectId.class);
    }


    /**
     * Returns true if the given object is a "mutable" type that needs to be enhanced
     * as DirtyableDBObject to prevent lost changes. Array types are ok because the default
     * isDirty method will detect changes there.
     *
     * @param object object to check
     *
     * @return true if mutable, false otherwise
     */
    public static boolean objectIsMutable(Object object) {
        if (object == null) {
            return false;
        }

        Class<?> clazz = object.getClass();

        return Collection.class.isAssignableFrom(clazz);
    }


    public static boolean needsNoConversion(Class clazz) {
        return NO_CONVERSION_CLASSES.contains(clazz);
    }


    public static Object fromObject(Class<?> type, final Object o) {
        if (o == null) {
            return null;
        } else if (type.isAssignableFrom(o.getClass())) {
            if (List.class.isAssignableFrom(type)) {
                if (DirtyableDBObjectList.class.isAssignableFrom(o.getClass())) {
                    return o;
                }

                return new DirtyableDBObjectList((List) o, true);
            } else if (Map.class.isAssignableFrom(type)) {
                if (DirtyableDBObjectMap.class.isAssignableFrom(o.getClass())) {
                    return o;
                }

                //noinspection unchecked
                return new DirtyableDBObjectMap((Map<String, Object>) o);
            } else if (Set.class.isAssignableFrom(type)) {
                if (DirtyableDBObjectSet.class.isAssignableFrom(o.getClass())) {
                    return o;
                }

                return new DirtyableDBObjectSet((Set) o, true);
            }

            return type.cast(o);
        } else if (Iterable.class.isAssignableFrom(o.getClass()) && Set.class.isAssignableFrom(type)) {
            throw new JeppettoException();
//            return fromSet(o, typeParameters);
        } else if (o instanceof DBObject) {
            DBObject copy = (DBObject) EnhancerHelper.getDirtyableDBObjectEnhancer(type).newInstance();

            copy.putAll((DBObject) o);

            return copy;
        } else if (Enum.class.isAssignableFrom(type) && String.class.isAssignableFrom(o.getClass())) {
            // noinspection unchecked
            return Enum.valueOf((Class<Enum>) type, (String) o);
        } else if (BigDecimal.class == type && Number.class.isAssignableFrom(o.getClass())) {
            return BigDecimal.valueOf(((Number) o).doubleValue());
        } else if (BigInteger.class == type && Number.class.isAssignableFrom(o.getClass())) {
            return BigInteger.valueOf(((Number) o).longValue());
        } else {
            throw new RuntimeException("Not sure how to convert a " + o + " to a " + type.getSimpleName());
        }
    }


    public static Object toDBObject(Object object) {
        // TODO: handle arrays.
        if (object == null
            || NO_CONVERSION_CLASSES.contains(object.getClass())
            || DirtyableDBObject.class.isAssignableFrom(object.getClass())) {
            return object;
        } else if (Map.class.isAssignableFrom(object.getClass())) {
            //noinspection unchecked
            return new DirtyableDBObjectMap((Map) object);
        } else if (List.class.isAssignableFrom(object.getClass())) {
            return new DirtyableDBObjectList((List) object, true);
        } else if (Set.class.isAssignableFrom(object.getClass())) {
            return new DirtyableDBObjectSet((Set) object, true);
        } else if (Iterable.class.isAssignableFrom(object.getClass())) {
            throw new RuntimeException("oops...");
//            return new DirtyableDBObjectList((Iterable) object);
        } else if (DBObject.class.isInstance(object)) {
            BasicDBObject dbo = new BasicDBObject();
            DBObject src = (DBObject) object;

            for (String key : src.keySet()) {
                Object rawObject = src.get(key);
                Object dboValue = (rawObject == null) ? null : toDBObject(rawObject);

                dbo.put(key, dboValue);
            }

            return dbo;
        } else if (Enum.class.isInstance(object)) {
            return ((Enum) object).name();
        } else {
            Enhancer enhancer = EnhancerHelper.getDirtyableDBObjectEnhancer(object.getClass());

            //noinspection unchecked
            return enhancer.enhance(object);
        }
    }
}
