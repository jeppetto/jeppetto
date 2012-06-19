/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao;


import org.iternine.jeppetto.dao.annotation.DataAccessMethod;
import org.iternine.jeppetto.enhance.ClassLoadingUtil;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class DAOBuilder {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private static final AtomicInteger count = new AtomicInteger(0);
    private static final Logger logger = LoggerFactory.getLogger(DAOBuilder.class);


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------


    public static <T, ID, I extends GenericDAO<T, ID>> I buildDAO(Class<T> modelClass,
                                                                  Class<I> daoInterface,
                                                                  Class<? extends QueryModelDAO<T, ID>> partialDAOClass,
                                                                  Map<String, Object> daoProperties) {
        return buildDAO(modelClass, daoInterface, partialDAOClass, daoProperties, null);
    }


    public static <T, ID, I extends GenericDAO<T, ID>> I buildDAO(Class<T> modelClass,
                                                                  Class<I> daoInterface,
                                                                  Class<? extends QueryModelDAO<T, ID>> partialDAOClass,
                                                                  Map<String, Object> daoProperties,
                                                                  AccessControlContextProvider accessControlContextProvider) {
        if (AccessControllable.class.isAssignableFrom(daoInterface)) {
            // Verify the DAO implementation can support AccessControllable...

            // ...if not assignable from AccessControllable, then fail...
            if (!AccessControllable.class.isAssignableFrom(partialDAOClass)) {
                throw new RuntimeException("Concrete DAO doesn't support AccessControllable (which is expected by DAO interface)");
            }

            // ...if no matching constructor, then fail...
            try {
                partialDAOClass.getDeclaredConstructor(Class.class, Map.class, AccessControlContextProvider.class);
            } catch (Exception e) {
                throw new RuntimeException("Concrete DAO doesn't support AccessControllable (which is expected by DAO interface)");
            }

            // TODO: validate AccessControllable methods exist

            if (accessControlContextProvider == null) {
                throw new RuntimeException("No AccessControlContextProvider specified.");
            }
        }

        Class<? extends I> fullDAOClass = completeDAO(modelClass, daoInterface, partialDAOClass, accessControlContextProvider != null);

        try {
            if (accessControlContextProvider != null) {
                return fullDAOClass.getDeclaredConstructor(Class.class, Map.class, AccessControlContextProvider.class).newInstance(modelClass,
                                                                                                                                   daoProperties,
                                                                                                                                   accessControlContextProvider);
            } else {
                return fullDAOClass.getDeclaredConstructor(Class.class, Map.class).newInstance(modelClass, daoProperties);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private static <T, ID, I extends GenericDAO<T, ID>> Class<? extends I> completeDAO(Class<T> modelClass,
                                                                                       Class<I> daoInterface,
                                                                                       Class<? extends QueryModelDAO<T, ID>> partialDAOClass,
                                                                                       boolean accessControlEnabled) {
        try {
            ClassPool pool = ClassPool.getDefault();

            pool.insertClassPath(new ClassClassPath(daoInterface));

            CtClass daoInterfaceCtClass = pool.get(daoInterface.getName());
            CtClass partialDAOCtClass = pool.get(partialDAOClass.getName());
            CtClass concrete = pool.makeClass(String.format("%s$%d", daoInterface.getName(), count.incrementAndGet()));

            concrete.setSuperclass(partialDAOCtClass);
            concrete.addInterface(daoInterfaceCtClass);

            String constructorCode;

            if (accessControlEnabled) {
                constructorCode = String.format("public %s(Class entityClass, java.util.Map daoProperties, org.iternine.jeppetto.dao.AccessControlContextProvider accessControlContextProvider) { " +
                                                "    super(entityClass, daoProperties, accessControlContextProvider); " +
                                                "}",
                                                concrete.getSimpleName());
            } else {
                constructorCode = String.format("public %s(Class entityClass, java.util.Map daoProperties) { " +
                                                "    super(entityClass, daoProperties); " +
                                                "}",
                                                concrete.getSimpleName());
            }

            concrete.addConstructor(CtNewConstructor.make(constructorCode, concrete));

            // implement all abstract methods to call the delegate dynamic dao
            for (CtMethod interfaceMethod : daoInterfaceCtClass.getMethods()) {
                try {
                    CtMethod daoMethod = partialDAOCtClass.getMethod(interfaceMethod.getName(), interfaceMethod.getSignature());

                    if (!Modifier.isAbstract(daoMethod.getModifiers())) {
                        continue;  // If the method has been implemented, we bypass.
                    }
                } catch (NotFoundException ignore) {
                    // If the method is not declared in the partial class, we fall through to implementation
                }

                implementMethod(concrete, interfaceMethod, modelClass, accessControlEnabled);
            }

            return ClassLoadingUtil.toClass(concrete);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static <T> void implementMethod(CtClass concrete, CtMethod interfaceMethod, Class<T> modelClass, boolean accessControlEnabled)
            throws CannotCompileException, ClassNotFoundException {
        CtMethod concreteMethod = CtNewMethod.copy(interfaceMethod, concrete, null);
        StringBuilder sb = new StringBuilder();
        DataAccessMethod dataAccessMethod;

        sb.append("{\n"
                  + "    java.util.Iterator argsIterator = java.util.Arrays.asList($args).iterator();\n"
                  + "    org.iternine.jeppetto.dao.QueryModel queryModel = new org.iternine.jeppetto.dao.QueryModel();\n\n");

        if ((dataAccessMethod = (DataAccessMethod) interfaceMethod.getAnnotation(DataAccessMethod.class)) != null) {
            buildQueryModelFromAnnotation(dataAccessMethod, sb);

            if (accessControlEnabled) {
                if (dataAccessMethod.useAccessControlContextArgument()) {
                    sb.append("    queryModel.setAccessControlContext((org.iternine.jeppetto.dao.AccessControlContext) argsIterator.next());\n\n");
                } else {
                    sb.append("    queryModel.setAccessControlContext(getAccessControlContextProvider().getCurrent());\n\n");
                }
            }

            if (dataAccessMethod.operation() == OperationType.Read) {
                buildReturnClause(interfaceMethod, sb, modelClass);
            } else {
                buildDeleteClause(sb);
            }
        } else {
            // deal w/ '...As()' case
            OperationType operationType = buildQueryModelFromMethodName(interfaceMethod.getName(), sb);

            if (accessControlEnabled) {
                if (interfaceMethod.getName().endsWith("As")) {
                    sb.append("    queryModel.setAccessControlContext((org.iternine.jeppetto.dao.AccessControlContext) argsIterator.next());\n\n");
                } else {
                    sb.append("    queryModel.setAccessControlContext(getAccessControlContextProvider().getCurrent());\n\n");
                }
            }

            if (operationType == OperationType.Read) {
                buildReturnClause(interfaceMethod, sb, modelClass);
            } else {
                buildDeleteClause(sb);
            }
        }

        sb.append('\n').append('}');

        if (logger.isDebugEnabled()) {
            logDerivedMethod(interfaceMethod, sb);
        }

        try {
            concreteMethod.setBody(sb.toString());
        } catch (CannotCompileException e) {
            throw new RuntimeException("Unable to add method:\n" + sb.toString(), e);
        }

        concrete.addMethod(concreteMethod);
    }


    private static void buildQueryModelFromAnnotation(DataAccessMethod dataAccessMethod, StringBuilder sb) {
        if (dataAccessMethod.conditions() != null && dataAccessMethod.conditions().length > 0) {
            for (org.iternine.jeppetto.dao.annotation.Condition conditionAnnotation : dataAccessMethod.conditions()) {
                sb.append(String.format("    queryModel.addCondition(buildCondition(\"%s\", org.iternine.jeppetto.dao.ConditionType.%s, argsIterator));\n",
                                        conditionAnnotation.field(), conditionAnnotation.type().name()));
            }

            sb.append('\n');
        }

        if (dataAccessMethod.associations() != null && dataAccessMethod.associations().length > 0) {
            for (org.iternine.jeppetto.dao.annotation.Association associationAnnotation : dataAccessMethod.associations()) {
                for (org.iternine.jeppetto.dao.annotation.Condition conditionAnnotation : associationAnnotation.conditions()) {
                    sb.append(String.format("    queryModel.addAssociationCondition(\"%s\", buildCondition(\"%s\", org.iternine.jeppetto.dao.ConditionType.%s, argsIterator));\n",
                                            associationAnnotation.field(), conditionAnnotation.field(), conditionAnnotation.type().name()));
                }
            }

            sb.append('\n');
        }

        if (dataAccessMethod.projections() != null && dataAccessMethod.projections().length > 0) {
            sb.append(String.format("    queryModel.setProjection(buildProjection(\"%s\", org.iternine.jeppetto.dao.ProjectionType.%s, argsIterator));\n\n",
                                    dataAccessMethod.projections()[0].field(), dataAccessMethod.projections()[0].type().name()));
        }

        if (dataAccessMethod.sorts() != null && dataAccessMethod.sorts().length > 0) {
            for (org.iternine.jeppetto.dao.annotation.Sort sort : dataAccessMethod.sorts()) {
                sb.append(String.format("    queryModel.addSort(org.iternine.jeppetto.dao.SortDirection.%s, \"%s\");\n", sort.direction().name(), sort.field()));
            }

            sb.append('\n');
        }

        if (dataAccessMethod.limitResults()) {
            sb.append("    queryModel.setMaxResults(((Integer) argsIterator.next()).intValue());\n\n");
        }

        if (dataAccessMethod.skipResults()) {
            sb.append("    queryModel.setFirstResult(((Integer) argsIterator.next()).intValue());\n\n");
        }
    }


    /**
     * We build 'findBy', 'countBy', and 'deleteBy' QueryModels in the following way:
     * <p/>
     *      findBy<query part>*[OrderBy<order part>*]
     *      countBy<query part>*[OrderBy<order part>*]
     *      deleteBy<query part>*
     * <p/>
     * Query parts are of the following forms:
     * <p/>
     *      <PropertyName> :                   column value must equal positional argument value
     *      <PropertyName>Equal :              column value must equal positional argument value
     *      <PropertyName>NotEqual :           column value must not equal positional argument value
     *      <PropertyName>GreaterThan :        column value must be greater than positional argument value
     *      <PropertyName>GreaterThanEqual :   column value must be greater than or equal to positional argument value
     *      <PropertyName>LessThan :           column value must be less than positional argument value
     *      <PropertyName>LessThanEqual :      column value must be less than or equal to positional argument value
     *      <PropertyName>Between :            column value must be between the next two positional argument values
     *      <PropertyName>Within :             column value must be in a java.util.Collection of values
     *      <PropertyName>NotWithin :          column value must not be in a java.util.Collection of values
     *      <PropertyName>IsNull :             column value must be null
     *      <PropertyName>IsNotNull :          column value must not be null
     * <p/>
     * Additionally, strings of the following form:
     * <p/>
     *      Having<AssociationName>With<association query part>*
     * <p/>
     * Can be specified to find results that have associations to other objects with the interpreted query parts.
     * Multiple associations can be specified, but note that each hangs off the root object, not each other
     * (e.g. List<Word> findByHavingFontWithColorHavingFormattingWithJustification() would assume an association
     * between Word and both Font and Formatting, not Word to Font to Formatting).
     * <p/>
     * Order parts are of the following forms:
     * <p/>
     *      <PropertyName>Asc :          order by the column value ascending
     *      <PropertyName>Desc :         order by the column value descending
     *      <PropertyName> :             order by the column value ascending
     *
     * @param methodName of the method to construct a QueryModel from
     * @param sb the StringBuilder to place the resulting logic into
     *
     * @return the OperationType that the methodName refers to.
     */
    private static OperationType buildQueryModelFromMethodName(String methodName, StringBuilder sb) {
        String queryString;
        OperationType operationType;

        if (methodName.startsWith("findBy")) {
            queryString = methodName.substring("findBy".length(),
                                               methodName.length() - (methodName.endsWith("As") ? "As".length() : 0));
            operationType = OperationType.Read;
        } else if (methodName.startsWith("countBy")) {
            sb.append("    queryModel.setProjection(buildProjection(null, org.iternine.jeppetto.dao.ProjectionType.RowCount, argsIterator));\n\n");

            queryString = methodName.substring("countBy".length(),
                                               methodName.length() - (methodName.endsWith("As") ? "As".length() : 0));
            operationType = OperationType.Read;
        } else if (methodName.startsWith("deleteBy")) {
            queryString = methodName.substring("deleteBy".length(),
                                               methodName.length() - (methodName.endsWith("As") ? "As".length() : 0));
            operationType = OperationType.Delete;
        } else {
            throw new UnsupportedOperationException("Don't know how to handle '" + methodName + "'");
        }

        int orderByIndex = queryString.indexOf("OrderBy");

        String[] queryParts;
        String orderParts;

        if (orderByIndex == -1) {
            queryParts = queryString.split("Having");
            orderParts = null;
        } else {
            queryParts = queryString.substring(0, orderByIndex).split("Having");
            orderParts = queryString.substring(orderByIndex + "OrderBy".length(), queryString.length());
        }

        if (queryParts != null) {
            if (queryParts[0].length() > 0) {
                String[] conditionStrings = queryParts[0].split("And");

                for (String conditionString : conditionStrings) {
                    String conditionName = getConditionNameFromString(conditionString);

                    sb.append(String.format("    queryModel.addCondition(buildCondition(\"%s\", org.iternine.jeppetto.dao.ConditionType.%s, argsIterator));\n",
                                            pruneFieldNameFromString(conditionString, conditionName), conditionName));
                }

                sb.append('\n');
            }

            for (int i = 1; i < queryParts.length; i++) {
                String associationString = queryParts[i];
                int withIndex = associationString.indexOf("With");  // If -1, exception.  Okay.
                String[] conditionStrings = associationString.substring(withIndex + 4, associationString.length()).split("And");

                for (String conditionString : conditionStrings) {
                    String conditionName = getConditionNameFromString(conditionString);

                    sb.append(String.format("    queryModel.addAssociationCondition(\"%s\", buildCondition(\"%s\", org.iternine.jeppetto.dao.ConditionType.%s, argsIterator));\n",
                                            Character.toLowerCase(associationString.charAt(0)) + associationString.substring(1, withIndex),
                                            pruneFieldNameFromString(conditionString, conditionName), conditionName));
                }

                sb.append('\n');
            }
        }

        if (orderParts != null && operationType == OperationType.Read) {
            for (String orderPart : orderParts.split("And")) {
                SortDirection sortDirection;
                String fieldName;

                if (orderPart.endsWith("Desc")) {
                    sortDirection = SortDirection.Descending;
                    fieldName = pruneFieldNameFromString(orderPart, "Desc");
                } else {
                    sortDirection = SortDirection.Ascending;
                    fieldName = pruneFieldNameFromString(orderPart, "Asc");
                }

                sb.append(String.format("    queryModel.addSort(org.iternine.jeppetto.dao.SortDirection.%s, \"%s\");\n",
                                        sortDirection.name(), fieldName));
            }

            sb.append('\n');
        }

        return operationType;
    }


    private static String getConditionNameFromString(String conditionString) {
        for (ConditionType conditionType : ConditionType.values()) {
            if (conditionString.endsWith(conditionType.name())) {
                return conditionType.name();
            }
        }

        // If we don't find a matching ConditionType, assume "Equal"
        return ConditionType.Equal.name();
    }


    private static String pruneFieldNameFromString(String conditionString, String trailingPart) {
        StringBuilder fieldName = new StringBuilder();

        if (conditionString.endsWith(trailingPart)) {
            fieldName.append(conditionString.substring(0, conditionString.length() - trailingPart.length()));
        } else {
            fieldName.append(conditionString);
        }

        fieldName.setCharAt(0, Character.toLowerCase(conditionString.charAt(0)));

        return fieldName.toString();
    }


    private static <T> void buildReturnClause(CtMethod method, StringBuilder sb, Class<T> modelClass) {
        try {
            String returnTypeName = method.getReturnType().getName();

            if (modelClass.getName().equals(returnTypeName)) {
                if (method.getExceptionTypes().length > 0) {
                    sb.append("\n    return ($r) findUniqueUsingQueryModel(queryModel);");
                } else {
                    sb.append(  "    try {\n"
                              + "        return ($r) findUniqueUsingQueryModel(queryModel);\n"
                              + "    } catch (org.iternine.jeppetto.dao.NoSuchItemException e) {\n"
                              + "        return null;\n"
                              + "    }");
                }
            } else if ("java.util.Set".equals(returnTypeName)) {
                sb.append(  "    java.util.Set result = new java.util.HashSet();\n"
                          + "    for (java.util.Iterator iterator = findUsingQueryModel(queryModel).iterator(); iterator.hasNext(); ) {\n"
                          + "        result.add(iterator.next());\n"
                          + "    }\n"
                          + "     \n"
                          + "    return result;");
            } else if ("java.util.List".equals(returnTypeName) || "java.util.Collection".equals(returnTypeName)) {
                sb.append(  "    java.util.List result = new java.util.ArrayList();\n"
                          + "    for (java.util.Iterator iterator = findUsingQueryModel(queryModel).iterator(); iterator.hasNext(); ) {\n"
                          + "        result.add(iterator.next());\n"
                          + "    }\n"
                          + "     \n"
                          + "    return result;");
            } else if ("java.lang.Iterable".equals(returnTypeName)) {
                sb.append(  "\n    return findUsingQueryModel(queryModel);");
            } else if ("int".equals(returnTypeName)) {
                sb.append(  "\n    return ((Number) projectUsingQueryModel(queryModel)).intValue();");
            } else if ("long".equals(returnTypeName)) {
                sb.append(  "\n    return ((Number) projectUsingQueryModel(queryModel)).longValue();");
            } else {
                sb.append(  "\n    return ($r) projectUsingQueryModel(queryModel);");
            }
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    private static void buildDeleteClause(StringBuilder sb) {
        sb.append("\n    deleteUsingQueryModel(queryModel);");
    }


    private static void logDerivedMethod(CtMethod interfaceMethod, StringBuilder sb) {
        try {
            String parameters = "";
            String exceptions = "\n        throws ";
            int parameterCount = 0;

            for (CtClass parameterType : interfaceMethod.getParameterTypes()) {
                if (parameters.length() > 0) {
                    parameters = parameters + ", ";
                }
                parameters = parameters + parameterType.getSimpleName() + " a" + parameterCount++;
            }

            for (CtClass exceptionType : interfaceMethod.getExceptionTypes()) {
                exceptions = exceptions + exceptionType.getSimpleName();
            }

            logger.debug(String.format("Adding DAO method implementation: \n\n"
                                       + "public %s %s(%s) %s %s\n\n",
                                       interfaceMethod.getReturnType().getSimpleName(),
                                       interfaceMethod.getName(),
                                       parameters,
                                       exceptions.length() > 17 ? exceptions : "",
                                       sb.toString()));
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
