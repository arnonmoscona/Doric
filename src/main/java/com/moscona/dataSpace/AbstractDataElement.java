/*
 * Copyright (c) 2015. Arnon Moscona
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.moscona.dataSpace;

import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.persistence.PersistenceType;

/**
 * Created: 12/9/10 5:21 PM
 * By: Arnon Moscona
 */
public abstract class AbstractDataElement implements IDataElement {
    private static final long serialVersionUID = -5094288149716645920L;
    private transient PersistenceType persistenceType = PersistenceType.MEMORY_ONLY;
    private transient INameSpace nameSpace = null; // the name space this belongs to (if any)
    private String name = null; // the name in the name space (if any)
    private String description = null;

    @Override
    public final PersistenceType getPersistenceType() {
        return persistenceType;
    }

    @Override
    public void setPersistenceType(PersistenceType persistenceType) {
        this.persistenceType = persistenceType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public INameSpace getNameSpace() {
        return nameSpace;
    }

    @Override
    public void setNameSpace(INameSpace nameSpace) throws DataSpaceException {
        this.nameSpace = nameSpace;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public IDataElement setDescription(String description) {
        this.description = description;
        return  this;
    }

    @Override
    public boolean isVector() {
        return false;
    }
}
