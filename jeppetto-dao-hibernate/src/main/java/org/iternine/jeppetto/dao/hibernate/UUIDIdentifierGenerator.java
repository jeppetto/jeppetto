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

package org.iternine.jeppetto.dao.hibernate;


import org.hibernate.HibernateException;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.id.IdentifierGenerator;

import java.io.Serializable;
import java.util.UUID;


public class UUIDIdentifierGenerator
        implements IdentifierGenerator {

    //-------------------------------------------------------------
    // Implementation - IdentifierGenerator
    //-------------------------------------------------------------

    public Serializable generate(SessionImplementor session, Object object)
            throws HibernateException {
        UUID uuid = UUID.randomUUID();
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toHexString(uuid.getMostSignificantBits()));
        sb.append(Long.toHexString(uuid.getLeastSignificantBits()));

        while (sb.length() < 32) {
            sb.append('0');
        }

        return sb.toString();
    }
}
