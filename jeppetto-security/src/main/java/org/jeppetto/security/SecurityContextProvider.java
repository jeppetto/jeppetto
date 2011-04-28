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

package org.jeppetto.security;


public interface SecurityContextProvider {

    /**
     * This method is used to retrieve a SecurityContext object.  The returned value contains information necessary to
     * authenticate the caller (managed by the service that invoked this method) and to provide information used to
     * limit the data and actions the authenticated user has access to.
     *
     * In the normal case, a securityContextKey is an externally known identifier associated with an actor that is
     * invoking a particular action on the system.
     *
     * Optionally, the securityContextKey can specify a targetIdentifier.  This mode allows for the scenario where
     * an actor wishes to perform an operation or take an action on behalf of another user.  In this case, this method
     * returns a SecurityContext object that has it's target modified.  The target can only be adjusted if the actor
     * has sufficient privileges.
     *
     * The form of the securityContextKey is:
     *
     *    actorIdentifier[:targetIdentifier]
     *
     * The choice of what to use for identifiers is left up to the implementation.
     *
     * @param securityContextKey a String specifying the desired SecurityContext
     *
     * @return SecurityContext (with a possibly modified target)
     *
     * @throws NoSuchSecurityContextException if either the actorIdentifier or targetIdentifier cannot be found
     */
    SecurityContext getSecurityContext(String securityContextKey)
            throws NoSuchSecurityContextException;
}
