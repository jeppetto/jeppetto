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

package org.iternine.jeppetto.dao;


import org.iternine.jeppetto.dao.annotation.DataAccessMethod;
import org.iternine.jeppetto.enhance.ClassLoadingUtil;

import com.yammer.metrics.core.TimerContext;
import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
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
        if (AccessControlDAO.class.isAssignableFrom(daoInterface)) {
            // Verify the DAO implementation can support AccessControlDAO...

            // ...if not assignable from AccessControlDAO, then fail...
            if (!AccessControlDAO.class.isAssignableFrom(partialDAOClass)) {
                throw new RuntimeException("Concrete DAO doesn't support AccessControlDAO (expected by the DAO interface)");
            }

            // ...if no matching constructor, then fail...
            try {
                partialDAOClass.getDeclaredConstructor(Class.class, Map.class, AccessControlContextProvider.class);
            } catch (Exception e) {
                throw new RuntimeException("Concrete DAO doesn't support AccessControlDAO (expected by the DAO interface)");
            }

            // TODO: validate AccessControlDAO methods exist

            if (accessControlContextProvider == null) {
                throw new RuntimeException("No AccessControlContextProvider specified.");
            }
        }

        Class<? extends I> fullDAOClass = completeDAO(modelClass, daoInterface, partialDAOClass, accessControlContextProvider != null,
                                                      daoProperties != null && Boolean.parseBoolean((String) daoProperties.get("enableMetrics")));

        try {
            if (accessControlContextProvider != null) {
                Constructor<? extends I> constructor = fullDAOClass.getDeclaredConstructor(Class.class, Map.class,
                                                                                           AccessControlContextProvider.class);

                return constructor.newInstance(modelClass, daoProperties, accessControlContextProvider);
            } else {
                Constructor<? extends I> constructor = fullDAOClass.getDeclaredConstructor(Class.class, Map.class);

                return constructor.newInstance(modelClass, daoProperties);
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
                                                                                       boolean accessControlEnabled,
                                                                                       boolean metricsEnabled) {
        try {
            ClassPool pool = ClassPool.getDefault();

            pool.insertClassPath(new ClassClassPath(daoInterface));

            CtClass fullDAOCtClass = pool.makeClass(String.format("%s$%d", daoInterface.getName(), count.incrementAndGet()));
            CtClass partialDAOCtClass = pool.get(partialDAOClass.getName());
            CtClass daoInterfaceCtClass = pool.get(daoInterface.getName());

            fullDAOCtClass.setSuperclass(partialDAOCtClass);
            fullDAOCtClass.addInterface(daoInterfaceCtClass);

            buildConstructor(fullDAOCtClass, accessControlEnabled);
            buildNeededMethods(fullDAOCtClass, partialDAOCtClass, daoInterfaceCtClass, modelClass, accessControlEnabled, metricsEnabled);

            return ClassLoadingUtil.toClass(fullDAOCtClass);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static void buildConstructor(CtClass fullDAOCtClass, boolean accessControlEnabled)
            throws CannotCompileException {
        String constructorCode;

        if (accessControlEnabled) {
            constructorCode = String.format("public %s(Class entityClass, java.util.Map daoProperties, org.iternine.jeppetto.dao.AccessControlContextProvider accessControlContextProvider) { " +
                                            "    super(entityClass, daoProperties, accessControlContextProvider); " +
                                            "}",
                                            fullDAOCtClass.getSimpleName());
        } else {
            constructorCode = String.format("public %s(Class entityClass, java.util.Map daoProperties) { " +
                                            "    super(entityClass, daoProperties); " +
                                            "}",
                                            fullDAOCtClass.getSimpleName());
        }

        fullDAOCtClass.addConstructor(CtNewConstructor.make(constructorCode, fullDAOCtClass));
    }


    private static <T> void buildNeededMethods(CtClass fullDAOCtClass, CtClass partialDAOCtClass, CtClass daoInterfaceCtClass,
                                               Class<T> modelClass, boolean accessControlEnabled, boolean metricsEnabled)
            throws CannotCompileException, ClassNotFoundException, NotFoundException {
        // Look through all methods to find which ones need to be implemented.
        for (CtMethod interfaceMethod : daoInterfaceCtClass.getMethods()) {
            try {
                CtMethod daoMethod = partialDAOCtClass.getMethod(interfaceMethod.getName(), interfaceMethod.getSignature());

                // The method is present in the partial class.
                if (!Modifier.isAbstract(daoMethod.getModifiers())) {
                    if (metricsEnabled && shouldAddMetricsToMethod(interfaceMethod, daoInterfaceCtClass)) {
                        logger.debug("Generating metrics delegate for method " + daoMethod.getName() + "()");

                        CtMethod delegator = CtNewMethod.delegator(daoMethod, fullDAOCtClass);

                        insertMetrics(fullDAOCtClass, delegator, daoInterfaceCtClass);

                        fullDAOCtClass.addMethod(delegator);
                    }

                    continue;
                }

                // If we're here, the method does not have a concrete implementation.  Fall through to implement it.
            } catch (NotFoundException ignore) {
                // If we're here, the method is not present in the partial class.  Fall through to implement it.
            }

            CtMethod daoMethod = implementMethod(fullDAOCtClass, interfaceMethod, modelClass, accessControlEnabled);

            if (metricsEnabled) {
                insertMetrics(fullDAOCtClass, daoMethod, daoInterfaceCtClass);
            }
        }
    }


    private static boolean shouldAddMetricsToMethod(CtMethod interfaceMethod, CtClass daoInterfaceCtClass) {
        // Check if the method is directly declared in the interface.  If yes, it was likely implemented for
        // performance or to accomplish something Jeppetto doesn't offer and we should add metrics.
        try {
            daoInterfaceCtClass.getDeclaredMethod(interfaceMethod.getName(), interfaceMethod.getParameterTypes());

            return true;
        } catch (NotFoundException ignore) {
        }

        // Check if the method is directly declared in the GenericDAO interface.  If yes, it is in the set of
        // common DAO methods that we want metrics for.
        try {
            ClassPool.getDefault().get(GenericDAO.class.getName()).getDeclaredMethod(interfaceMethod.getName(),
                                                                                     interfaceMethod.getParameterTypes());

            return true;
        } catch (NotFoundException ignore) {
        }

        return false;
    }


    private static void insertMetrics(CtClass fullDAOCtClass, CtMethod daoMethod, CtClass daoInterfaceCtClass)
            throws CannotCompileException, NotFoundException {
        final String timerField = createTimerField(fullDAOCtClass, daoMethod, daoInterfaceCtClass);

        logger.debug("Adding metrics to method " + daoMethod.getName() + "()");

        daoMethod.addLocalVariable("__tc", ClassPool.getDefault().get(TimerContext.class.getName()));
        daoMethod.insertBefore("__tc = this." + timerField + ".time();");
        daoMethod.insertAfter("__tc.stop();", false);
    }


    private static String createTimerField(CtClass fullCtClass, CtMethod daoMethod, CtClass daoInterfaceCtClass)
            throws CannotCompileException {
        String timerField = "__" + daoMethod.getName() + "Timer";
        String timerDeclaration = "private final com.yammer.metrics.core.Timer " + timerField
                                  + "  = com.yammer.metrics.Metrics.newTimer(" + daoInterfaceCtClass.getName() + ".class, "
                                  +                                          "\"" + daoMethod.getName() + "\");";

        logger.debug("Adding Timer field: " + timerField);

        fullCtClass.addField(CtField.make(timerDeclaration, fullCtClass));

        return timerField;
    }


    private static <T> CtMethod implementMethod(CtClass fullDAOCtClass, CtMethod interfaceMethod,
                                                Class<T> modelClass, boolean accessControlEnabled)
            throws CannotCompileException, ClassNotFoundException {
        CtMethod daoMethod = CtNewMethod.copy(interfaceMethod, fullDAOCtClass, null);
        StringBuilder sb = new StringBuilder();
        DataAccessMethod dataAccessMethod;
        OperationType operationType;

        sb.append("{\n"
                  + "    java.util.Iterator argsIterator = java.util.Arrays.asList($args).iterator();\n"
                  + "    org.iternine.jeppetto.dao.QueryModel queryModel = new org.iternine.jeppetto.dao.QueryModel();\n\n");

        if ((dataAccessMethod = (DataAccessMethod) interfaceMethod.getAnnotation(DataAccessMethod.class)) != null) {
            operationType = buildQueryModelFromAnnotation(dataAccessMethod, sb);

            if (accessControlEnabled) {
                if (dataAccessMethod.useAccessControlContextArgument()) {
                    sb.append("    queryModel.setAccessControlContext((org.iternine.jeppetto.dao.AccessControlContext) argsIterator.next());\n\n");
                } else {
                    sb.append("    queryModel.setAccessControlContext(getAccessControlContextProvider().getCurrent());\n\n");
                }
            }
        } else {
            // deal w/ '...As()' case
            operationType = buildQueryModelFromMethodName(interfaceMethod.getName(), sb);

            if (accessControlEnabled) {
                if (interfaceMethod.getName().endsWith("As")) {
                    sb.append("    queryModel.setAccessControlContext((org.iternine.jeppetto.dao.AccessControlContext) argsIterator.next());\n\n");
                } else {
                    sb.append("    queryModel.setAccessControlContext(getAccessControlContextProvider().getCurrent());\n\n");
                }
            }
        }

        switch (operationType) {
        case Read:
            buildReturnClause(interfaceMethod, sb, modelClass);

            break;
        case Update:
            buildUpdateClause(sb);

            break;
        case Delete:
            buildDeleteClause(sb);

            break;
        }

        sb.append('\n').append('}');

        if (logger.isDebugEnabled()) {
            logDerivedMethod(interfaceMethod, sb);
        }

        try {
            daoMethod.setBody(sb.toString());
        } catch (CannotCompileException e) {
            throw new RuntimeException("Unable to add method:\n" + sb.toString(), e);
        }

        fullDAOCtClass.addMethod(daoMethod);

        return daoMethod;
    }


    private static OperationType buildQueryModelFromAnnotation(DataAccessMethod dataAccessMethod, StringBuilder sb) {
        if (dataAccessMethod.operation() == OperationType.Update) {
            sb.append("    org.iternine.jeppetto.dao.updateobject.UpdateObject updateObject = (org.iternine.jeppetto.dao.updateobject.UpdateObject) argsIterator.next();\n\n");
        }

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

        return dataAccessMethod.operation();
    }


    /**
     * We build 'findBy', 'countBy', 'updateBy', and 'deleteBy' QueryModels in the following way:
     * <p/>
     *      findBy<query part>*[OrderBy<order part>*][AndLimit][AndSkip]
     *      countBy<query part>*[OrderBy<order part>*][AndLimit][AndSkip]
     *      updateBy<query part>*
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
     * <p/>
     * Limiting the result size and pagination are indicated by the AndLimit and AndSkip phrases.  These must be
     * at the end of the DAO method name, and be in that order.  It is acceptable to omit one or the other if it
     * isn't needed.  Both clauses expect to find an integer value in the parameter list after all the other
     * parameters are specified.  For example, to paginate through a potentially long list of people with the same last
     * name, one could declare a method findBySurnameAndLimitAndSkip(String surname, int limitCount, int skipCount)
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
            queryString = methodName.substring("findBy".length(), methodName.length() - (methodName.endsWith("As") ? "As".length() : 0));
            operationType = OperationType.Read;
        } else if (methodName.startsWith("countBy")) {
            sb.append("    queryModel.setProjection(buildProjection(\"\", org.iternine.jeppetto.dao.ProjectionType.RowCount, argsIterator));\n\n");

            queryString = methodName.substring("countBy".length(), methodName.length() - (methodName.endsWith("As") ? "As".length() : 0));
            operationType = OperationType.Read;
        } else if (methodName.startsWith("updateBy")) {
            queryString = methodName.substring("updateBy".length(), methodName.length() - (methodName.endsWith("As") ? "As".length() : 0));
            operationType = OperationType.Update;
            sb.append("    org.iternine.jeppetto.dao.updateobject.UpdateObject updateObject = (org.iternine.jeppetto.dao.updateobject.UpdateObject) argsIterator.next();\n\n");
        } else if (methodName.startsWith("deleteBy")) {
            queryString = methodName.substring("deleteBy".length(), methodName.length() - (methodName.endsWith("As") ? "As".length() : 0));
            operationType = OperationType.Delete;
        } else {
            throw new UnsupportedOperationException("Don't know how to handle '" + methodName + "'");
        }

        int orderByIndex = queryString.indexOf("OrderBy");
        boolean limitResults;
        boolean skipResults;

        if (skipResults = queryString.endsWith("AndSkip")) {
            queryString = queryString.substring(0, queryString.length() - "AndSkip".length());
        }

        if (limitResults = queryString.endsWith("AndLimit")) {
            queryString = queryString.substring(0, queryString.length() - "AndLimit".length());
        }

        String[] queryParts;
        String orderParts;

        if (orderByIndex == -1) {
            queryParts = queryString.split("Having");
            orderParts = null;
        } else {
            queryParts = queryString.substring(0, orderByIndex).split("Having");
            orderParts = queryString.substring(orderByIndex + "OrderBy".length());
        }

        if (queryParts[0] != null) {
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

        if (limitResults) {
            sb.append("    queryModel.setMaxResults(((Integer) argsIterator.next()).intValue());\n\n");
        }

        if (skipResults) {
            sb.append("    queryModel.setFirstResult(((Integer) argsIterator.next()).intValue());\n\n");
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
            } else if ("double".equals(returnTypeName)) {
                sb.append(  "\n    return ((Number) projectUsingQueryModel(queryModel)).doubleValue();");
            } else {
                if (Iterable.class.isAssignableFrom(Class.forName(returnTypeName))) {
                    sb.append(  "\n    return ($r) findUsingQueryModel(queryModel);");
                } else {
                    sb.append("\n    return ($r) projectUsingQueryModel(queryModel);");
                }
            }
        } catch (NotFoundException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    private static void buildUpdateClause(StringBuilder sb) {
        sb.append("\n    return updateUsingQueryModel(updateObject, queryModel);");
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
