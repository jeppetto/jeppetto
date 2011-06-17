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

package org.jeppetto.dao.mongodb;


import org.jeppetto.dao.mongodb.enhance.Dirtyable;

import com.mongodb.DBObject;
import com.mongodb.MongoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


class MongoDBSession {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final Map<String, MongoDBSessionCache> caches = new HashMap<String, MongoDBSessionCache>();
    private final Map<MongoDBQueryModelDAO<?>, Map<DBObject, Object>> savedPerDAO = new HashMap<MongoDBQueryModelDAO<?>, Map<DBObject, Object>>();
    private final Map<MongoDBQueryModelDAO<?>, Collection<DBObject>> deletedPerDAO = new HashMap<MongoDBQueryModelDAO<?>, Collection<DBObject>>();
    private final Deque<SessionEntryPoint> creators = new ArrayDeque<SessionEntryPoint>();


    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private static final ThreadLocal<MongoDBSession> LOCAL = new ThreadLocal<MongoDBSession>();
    private static final Logger logger = LoggerFactory.getLogger(MongoDBSession.class);


    //-------------------------------------------------------------
    // Methods - Package - Static
    //-------------------------------------------------------------

    static boolean isActive() {
        return LOCAL.get() != null;
    }


    static void create() {
        create(logger, "Unknown Context");
    }


    static void create(Logger contextLogger, String contextName) {
        if (!isActive()) {
            logger.debug("Creating new MongoDBSession.");

            LOCAL.set(new MongoDBSession());
        }

        LOCAL.get().enter(contextLogger, contextName);
    }


    static void remove() {
        if (isActive()) {
            if (LOCAL.get().exit()) {
                LOCAL.remove();
            }
        }
    }


    static <T> void trackForSave(MongoDBQueryModelDAO<T> mongoDBQueryModelDAO, DBObject identifier, T entity, DBObject... cacheKeys) {
        validateState();

        MongoDBSession mongoDBSession = LOCAL.get();

        Map<DBObject, Object> savedEntities = mongoDBSession.savedPerDAO.get(mongoDBQueryModelDAO);

        if (savedEntities == null) {
            savedEntities = new LinkedHashMap<DBObject, Object>();

            mongoDBSession.savedPerDAO.put(mongoDBQueryModelDAO, savedEntities);
        }

        logger.debug("Tracking for save: {} = {}", identifier, entity);

        Collection<DBObject> deletedIdentifiers;
        if ((deletedIdentifiers = mongoDBSession.deletedPerDAO.get(mongoDBQueryModelDAO)) == null
            || !deletedIdentifiers.contains(identifier)) {
            savedEntities.put(identifier, entity);

            MongoDBSessionCache sessionCache = getCache(mongoDBQueryModelDAO.getDbCollection().getName());
            for (DBObject cacheKey : cacheKeys) {
                sessionCache.put(cacheKey, entity);
            }
        }
    }


    static void trackForDelete(MongoDBQueryModelDAO<?> mongoDBQueryModelDAO, DBObject identifier) {
        validateState();

        MongoDBSession mongoDBSession = LOCAL.get();

        Collection<DBObject> deletedEntityIdentifiers = mongoDBSession.deletedPerDAO.get(mongoDBQueryModelDAO);

        if (logger.isDebugEnabled()) {
            // TODO: check upsert list?
            if (deletedEntityIdentifiers.contains(identifier)) {
                logger.debug("Object already tracked for remove: {}", identifier);
            } else {
                logger.debug("Tracking for remove: {}", identifier);
            }
        }

        deletedEntityIdentifiers.add(identifier);

        if (mongoDBSession.savedPerDAO.get(mongoDBQueryModelDAO) != null) {
            mongoDBSession.savedPerDAO.get(mongoDBQueryModelDAO).remove(identifier);
        }
    }


    static void flush() {
        validateState();

        MongoDBSession mongoDBSession = LOCAL.get();

        // check to see if this is a re-entrant session
        if (mongoDBSession.creators.size() > 1) {
            return;
        }

        try {
            Set<MongoDBQueryModelDAO<?>> daoSet = new HashSet<MongoDBQueryModelDAO<?>>();

            daoSet.addAll(mongoDBSession.savedPerDAO.keySet());
            daoSet.addAll(mongoDBSession.deletedPerDAO.keySet());

            for (MongoDBQueryModelDAO<?> mongoDBQueryModelDAO : daoSet) {
                mongoDBSession.doFlush(mongoDBQueryModelDAO);
            }
        } finally {
            mongoDBSession.clear();
        }
    }


    static void flush(MongoDBQueryModelDAO<?> mongoDBQueryModelDAO) {
        validateState();

        LOCAL.get().doFlush(mongoDBQueryModelDAO);
    }


    static Object getObjectFromCache(String type, DBObject query) {
        return getCache(type).get(query);
    }


    //-------------------------------------------------------------
    // Methods - Private - Static
    //-------------------------------------------------------------

    private static void validateState() {
        if (!isActive()) {
            throw new IllegalStateException("Session not active.");
        }
    }


    private static MongoDBSessionCache getCache(String type) {
        validateState();

        MongoDBSession mongoDBSession = LOCAL.get();
        MongoDBSessionCache sessionCache = mongoDBSession.caches.get(type);

        if (sessionCache != null) {
            return sessionCache;
        } else {
            mongoDBSession.caches.put(type, new MongoDBSessionCache());

            return mongoDBSession.caches.get(type);
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void enter(Logger logger, String name) {
        int offset = 3;
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        StackTraceElement creator = stackTrace[Math.min(offset, stackTrace.length - 1)];

        creators.push(new SessionEntryPoint(creator, logger, name));
    }


    /**
     * @return true if session can now removed from thread local storage
     */
    private boolean exit() {
        boolean last = creators.size() == 1;

        try {
            if (!last) {
                String context = creators.peek().getName();

                logger.debug("Leaving re-entrant session created by {}", context);
            }

            creators.pop();

            if (last) {
                logger.debug("Removing MongoDBSession");

                if (logger.isDebugEnabled()) {

                    for (Map.Entry<MongoDBQueryModelDAO<?>, Map<DBObject, Object>> updatedPerDAOEntry : savedPerDAO.entrySet()) {
                        for (Object entity : updatedPerDAOEntry.getValue().values()) {
                            if (entity instanceof Dirtyable && ((Dirtyable) entity).isDirty()) {
                                logger.warn("{} is still dirty: {}", updatedPerDAOEntry.getKey().getClass(), entity);
                             }
                        }
                    }

                    for (Collection<DBObject> deletedEntityIdentifiers : deletedPerDAO.values()) {
                        for (DBObject deletedEntityIdentifier : deletedEntityIdentifiers) {
                            logger.debug("Removing {} before delete due to session close.", deletedEntityIdentifier);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            try {
                logger.error("Error while removing session.", t);
            } catch (Throwable t2) {
                // bury
            }
        }

        return last;
    }


    private void clear() {
        savedPerDAO.clear();
        caches.clear();
        deletedPerDAO.clear();
    }


    private void doFlush(MongoDBQueryModelDAO<?> mongoDBQueryModelDAO) {
        String contextName = creators.peek().getName();
        Logger contextLogger = creators.peek().getLogger();
        long dirtyCheckCost = 0L;
        int saveCount = 0;
        int upsertCount = 0;
        int deleteCount = 0;

        // TODO : perform actions in order they're tracked

        if (savedPerDAO.containsKey(mongoDBQueryModelDAO)) {
            // use get to keep objects in session
            Map<DBObject, Object> updatedEntities = savedPerDAO.get(mongoDBQueryModelDAO);

            for (Map.Entry<DBObject, Object> entry : updatedEntities.entrySet()) {
                long beforeDirtyCheck = System.nanoTime();

                if (!(entry.getValue() instanceof DBObject)) {
                    logger.error("Non-DBObject made it into the session: {}", entry.getValue());

                    continue;
                }

                try {
                    if (entry.getValue() instanceof Dirtyable) {
                        if (!((Dirtyable) entry.getValue()).isDirty()) {
                            dirtyCheckCost += (System.nanoTime() - beforeDirtyCheck);

                            continue;
                        }
                    }

                    // add here too for when object falls through condition above
                    dirtyCheckCost += (System.nanoTime() - beforeDirtyCheck);

                    mongoDBQueryModelDAO.trueSave(entry.getKey(), (DBObject) entry.getValue());
                    saveCount++;
                } catch (MongoException e) {
                    logger.error("Error saving {}.", entry.getValue(), e); // TODO: Implement
                } catch (UniquenessViolationRuntimeException e) {
                    logger.warn("Error saving {}. Duplicate record found.", entry.getValue());
                }
            }
            
            updatedEntities.clear();
        }

        if (deletedPerDAO.containsKey(mongoDBQueryModelDAO)) {
            Collection<DBObject> deletedEntityIdentifiers = deletedPerDAO.get(mongoDBQueryModelDAO);

            for (Iterator<DBObject> iterator = deletedEntityIdentifiers.iterator(); iterator.hasNext(); ) {
                DBObject deletedEntityIdentifier = iterator.next();

                logger.debug("Remove: {}", deletedEntityIdentifier.toMap());

                try {
                    mongoDBQueryModelDAO.trueRemove(deletedEntityIdentifier);
                    iterator.remove();

                    deleteCount++;
                } catch (MongoException e) {
                    logger.error("Error removing {}.", deletedEntityIdentifier, e); // TODO: Implement
                }
            }
        }

        getCache(mongoDBQueryModelDAO.getDbCollection().getName()).clear();

        contextLogger.debug("{} flushed {}s in {}ms. (save={}, upsert={}, delete={})",
                            new Object[] { contextName,
                                           mongoDBQueryModelDAO.getCollectionClass().getSimpleName(),
                                           TimeUnit.NANOSECONDS.toMillis(dirtyCheckCost),
                                           saveCount,
                                           upsertCount,
                                           deleteCount } );
    }


    //-------------------------------------------------------------
    // Inner Classes
    //-------------------------------------------------------------

    private static final class SessionEntryPoint {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private StackTraceElement stackTraceElement;
        private Logger logger;
        private String name;


        //-------------------------------------------------------------
        // Constructors
        //-------------------------------------------------------------

        public SessionEntryPoint(StackTraceElement stackTraceElement, Logger logger, String name) {
            this.stackTraceElement = stackTraceElement;
            this.logger = logger;
            this.name = name;
        }


        //-------------------------------------------------------------
        // Methods - Getters
        //-------------------------------------------------------------

        public StackTraceElement getStackTraceElement() {
            return stackTraceElement;
        }


        public Logger getLogger() {
            return logger;
        }


        public String getName() {
            return name;
        }
    }
}
