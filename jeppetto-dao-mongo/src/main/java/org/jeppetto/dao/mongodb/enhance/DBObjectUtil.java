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

package org.jeppetto.dao.mongodb.enhance;


import org.jeppetto.enhance.Enhancer;

import com.google.common.base.Function;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBPointer;
import com.mongodb.DBRefBase;
import org.bson.types.ObjectId;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
                           DBPointer.class,
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


    public static Object fromObject(Class<?> type, final Object o, final Class<?>... typeParameters) {
        if (o == null) {
            return null;
        } else if (type.isAssignableFrom(o.getClass())) {
            if (List.class.isAssignableFrom(type)) {
                return fromList(o, typeParameters);
            } else if (Map.class.isAssignableFrom(type)) {
                return fromMap(o, typeParameters);
            } else if (Set.class.isAssignableFrom(type)) {
                return fromSet(o, typeParameters);
            } else if (Iterable.class.isAssignableFrom(type)) {
                return fromList(o, typeParameters);
            }

            return type.cast(o);
        } else if (Iterable.class.isAssignableFrom(o.getClass()) && Set.class.isAssignableFrom(type)) {
            return fromSet(o, typeParameters);
        } else if (o instanceof DBObject) {
            // TODO: validate works on nested objects of various types...
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


    public static Object toDBObject(Class<?> cls, Object obj, Type... optionalValueTypes) {
        return prepareObjectForMongo(new TypeDefinition(cls, optionalValueTypes), obj);
    }


    public static Object toDBObject(Class<?> cls, boolean obj, Type... optionalValueTypes) {
        return obj;
    }


    public static Object toDBObject(Class<?> cls, byte obj, Type... optionalValueTypes) {
        return obj;
    }


    public static Object toDBObject(Class<?> cls, short obj, Type... optionalValueTypes) {
        return obj;
    }


    public static Object toDBObject(Class<?> cls, int obj, Type... optionalValueTypes) {
        return obj;
    }


    public static Object toDBObject(Class<?> cls, long obj, Type... optionalValueTypes) {
        return obj;
    }


    public static Object toDBObject(Class<?> cls, float obj, Type... optionalValueTypes) {
        return obj;
    }


    public static Object toDBObject(Class<?> cls, double obj, Type... optionalValueTypes) {
        return obj;
    }


    //-------------------------------------------------------------
    // Methods - Private - Static
    //-------------------------------------------------------------

    private static Object prepareObjectForMongo(TypeDefinition typeDef, Object object) {
        if (object == null) {
            return null;
        } else if (needsNoConversion(typeDef.getClazz(), object)
                   || Map.class.isInstance(object)
                   || Iterable.class.isInstance(object)) {
            if (mayHaveMembersThatNeedConversion(object)) {
                return prepareObjectMembersForMongo(typeDef, object);
            } else {
                return object;
            }
        } else if (Enum.class.isInstance(object)) {
            return ((Enum) object).name();
        } else {
            Enhancer enhancer = EnhancerHelper.getDirtyableDBObjectEnhancer(object.getClass());

            //noinspection unchecked
            return enhancer.enhance(object);
        }
    }


    private static boolean mayHaveMembersThatNeedConversion(Object obj) {
        return DBObject.class.isInstance(obj) || Map.class.isInstance(obj) || Iterable.class.isInstance(obj);
    }


    @SuppressWarnings({"unchecked"})
    private static Object prepareObjectMembersForMongo(TypeDefinition typeDef, Object object) {
        Class<?> cls = typeDef.getClazz();
        Class<?>[] optionalValueTypes = typeDef.getTypeParameters();

        if (DBObject.class.isAssignableFrom(cls)) {
            BasicDBObject dbo = new BasicDBObject();
            DBObject src = (DBObject) object;

            for (String key : src.keySet()) {
                Object rawObject = src.get(key);
                Object dboValue = (rawObject == null) ? null : toDBObject(rawObject.getClass(), rawObject);

                dbo.put(key, dboValue);
            }

            return dbo;
        } else if (Map.class.isAssignableFrom(cls)) {
            return wrapMap(coalesceTypeParam(optionalValueTypes, 1), (Map) object);
        } else if (List.class.isAssignableFrom(cls)) {
            return wrapList(coalesceTypeParam(optionalValueTypes, 0), (List) object);
        } else if (Set.class.isAssignableFrom(cls)) {
            return wrapSet(coalesceTypeParam(optionalValueTypes, 0), (Set) object);
        } else if (Iterable.class.isAssignableFrom(cls)) {
            return wrapList(coalesceTypeParam(optionalValueTypes, 0), (Iterable) object);
        } else {
            throw new IllegalStateException("Unanticipated object " + object + " and type " + cls);
        }
    }


    private static Class<?> coalesceTypeParam(Type[] classes, int index) {
        return (Class<?>) (classes == null || index >= classes.length ? Object.class : classes[index]);
    }


    private static Function<Object, Object> fromObjectFunction(final Class<?> type, final Class<?>... typeParams) {
        return new Function<Object, Object>() {
            @Override
            public Object apply(Object from) {
                return fromObject(type, from, typeParams);
            }
        };
    }


    private static Map<Object, Object> wrapMap(final Type valueType, final Map<Object, Object> delegate) {
        return new ForwardingMap<Object, Object>() {

            @Override
            protected Map<Object, Object> delegate() {
                return delegate;
            }

            @Override
            public Object get(Object key) {
                return toDBObject((Class<?>) valueType, super.get(key));
            }

            @Override
            public Set<Entry<Object, Object>> entrySet() {
                return ImmutableSet.copyOf(Iterables.transform(super.entrySet(), new Function<Entry<Object, Object>, Entry<Object, Object>>() {
                    @Override
                    public Entry<Object, Object> apply(Entry<Object, Object> from) {
                       return new AbstractMap.SimpleEntry<Object, Object>(from.getKey(), toDBObject((Class<?>) valueType, from.getValue()));
                    }
                }));
            }
        };
    }


    private static List<Object> wrapList(final Type valueType, final Iterable<Object> delegate) {
        final List<Object> delegateList = Lists.newArrayList(delegate);

        return new ForwardingList<Object>() {
            @Override
            protected List<Object> delegate() {
                return delegateList;
            }

            @Override
            public Object get(int index) {
                return toDBObject((Class<?>) valueType, super.get(index));
            }


            @Override
            public Iterator<Object> iterator() {
                final Iterator<Object> delegateIterator = delegateList.iterator();

                return new Iterator<Object>() {
                    @Override
                    public boolean hasNext() {
                        return delegateIterator.hasNext();
                    }


                    @Override
                    public Object next() {
                        return toDBObject((Class<?>) valueType, delegateIterator.next());
                    }


                    @Override
                    public void remove() {
                        delegateIterator.remove();
                    }
                };
            }
        };
    }


    private static List<Object> wrapSet(final Type valueType, final Set<Object> delegate) {
        return wrapList(valueType, Lists.newArrayList(delegate));
    }


    private static boolean needsNoConversion(Class<?> cls, Object obj) {
        return obj == null
               || cls.isArray()
               || NO_CONVERSION_CLASSES.contains(cls)
               || NO_CONVERSION_CLASSES.contains(obj.getClass())
               || (DBObject.class.isAssignableFrom(cls) && cls.isInstance(obj));
    }


    private static Map<?, ?> fromMap(final Object source, final Class<?>... typeParameters) {
        if (DirtyableDBObjectMap.class.isAssignableFrom(source.getClass())) {
            return (Map<?, ?>) source;
        }

        Function<Object, Object> valueFunction = fromObjectFunction(coalesceTypeParam(typeParameters, 1));

        if (String.class != coalesceTypeParam(typeParameters, 0)) {
            Function<Object, Object> keyFunction = fromObjectFunction(coalesceTypeParam(typeParameters, 0));
            Map<?, ?> sourceMap = (Map<?, ?>) source;
            DirtyableDBObjectMap converted = new DirtyableDBObjectMap();

            for (Map.Entry<?, ?> entry : sourceMap.entrySet()) {
                converted.put(keyFunction.apply(entry.getKey()), valueFunction.apply(entry.getValue()));
            }

            converted.markCurrentAsClean();
            
            return converted;
        } else {
            return new DirtyableDBObjectMap(Maps.transformValues((Map<?, ?>) source, valueFunction));
        }
    }


    private static Set<?> fromSet(final Object source, final Class<?>... typeParameters) {
        if (DirtyableDBObjectSet.class.isAssignableFrom(source.getClass())) {
            return (Set<?>) source;
        }

        Function<Object, Object> valueFunction = fromObjectFunction(coalesceTypeParam(typeParameters, 0));

        return new DirtyableDBObjectSet(Sets.newHashSet(Iterables.transform((Set<?>) source, valueFunction)));
    }


    private static List<?> fromList(final Object source, final Class<?>... typeParameters) {
        if (DirtyableDBObjectList.class.isAssignableFrom(source.getClass())) {
            return (List<?>) source;
        }

        Function<Object, Object> valueFunction = fromObjectFunction(coalesceTypeParam(typeParameters, 0));

        return new DirtyableDBObjectList(Lists.transform((List<?>) source, valueFunction));
    }


    //-------------------------------------------------------------
    // Inner Classes
    //-------------------------------------------------------------

    private static final class TypeDefinition {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private final Class<?> clazz;
        private final Class<?>[] typeParameters;


        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        public TypeDefinition(Class<?> clazz, Type... typeParametersAsType) {
            this.clazz = clazz;
            this.typeParameters = new Class<?>[typeParametersAsType.length];
            for (int i = 0; i < this.typeParameters.length; i++) {
                this.typeParameters[i] = (Class<?>) typeParametersAsType[i];
            }
        }


        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        public Class<?> getClazz() {
            return clazz;
        }


        public Class<?>[] getTypeParameters() {
            return typeParameters;
        }
    }
}
