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

package org.iternine.jeppetto.dao.test.accesscontrol;


import org.iternine.jeppetto.dao.AccessType;
import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.annotation.AccessControl;
import org.iternine.jeppetto.dao.annotation.Accessor;
import org.iternine.jeppetto.dao.annotation.Creator;


@AccessControl(
    creators = { @Creator( type = Creator.Type.Identified, grantedAccess = AccessType.ReadWrite ) },
    accessors = { @Accessor( type = Accessor.Type.Role, typeValue = "Administrator", access = AccessType.ReadWrite ) }
)
public interface IdentifiedCreatableObjectDAO extends GenericDAO<IdentifiedCreatableObject, String> {
}
