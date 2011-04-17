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

package org.jeppetto.enhance;


import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.log.LogChute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SLF4JLogChute
        implements LogChute {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private static Logger logger = LoggerFactory.getLogger("Velocity");


    //-------------------------------------------------------------
    // Implementation - LogChute
    //-------------------------------------------------------------

    @Override
    public void init(RuntimeServices runtimeServices)
            throws Exception {
        // Nothing
    }


    @Override
    public void log(int level, String message) {
        switch (level) {
        case TRACE_ID:
            logger.trace(message);
            break;

        case DEBUG_ID:
            logger.debug(message);
            break;

        case INFO_ID:
            logger.info(message);
            break;

        case WARN_ID:
            logger.warn(message);
            break;

        case ERROR_ID:
            logger.error(message);
            break;
        }
    }


    @Override
    public void log(int level, String message, Throwable throwable) {
        switch (level) {
        case TRACE_ID:
            logger.trace(message, throwable);
            break;

        case DEBUG_ID:
            logger.debug(message, throwable);
            break;

        case INFO_ID:
            logger.info(message, throwable);
            break;

        case WARN_ID:
            logger.warn(message, throwable);
            break;

        case ERROR_ID:
            logger.error(message, throwable);
            break;
        }
    }


    @Override
    public boolean isLevelEnabled(int level) {
        switch (level) {
        case TRACE_ID:
            return logger.isTraceEnabled();

        case DEBUG_ID:
            return logger.isDebugEnabled();

        case INFO_ID:
            return logger.isInfoEnabled();

        case WARN_ID:
            return logger.isWarnEnabled();

        case ERROR_ID:
            return logger.isErrorEnabled();
        }

        return false;
    }
}
