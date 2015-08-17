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
import com.moscona.dataSpace.persistence.IDataStore;
import com.moscona.dataSpace.persistence.PersistenceType;

import java.util.List;

/**
 * Created: 12/9/10 7:02 PM
 * By: Arnon Moscona
 * An bundle of data items with names and a default persistence type
 */
public interface INameSpace {
    public PersistenceType getDefaultPersistenceType();
    public IDataStore getDataStore();
    public void setName(String name);
    public String getName();

    /**
     * validates the naming convention and that there is no other variable of this name in the name space
     * @param candidateName
     * @return
     * @throws DataSpaceException
     */
    public String validateName(String candidateName) throws DataSpaceException;

    /**
     * Adds the data element to the name space and promotes it to the default persistence type if it was in a lower persistence type
     * @param name
     * @param value
     */
    public void assign(String name, IDataElement value) throws DataSpaceException;

    /**
     * Adds a value without assigning it a name. It will actually end up with a name, but that is unknown to the caller.
     * Really only useful in non-persistent name spaces and therefore persistent values are prohibited.
     * Note that you can get around this, but you don't want to. If you want persistent items it is best to assign them
     * to a persistent name space.
     * @param anonymous
     * @throws DataSpaceException if the data element was persistent
     */
    public String add(IDataElement anonymous) throws DataSpaceException;

    /**
     * Retrieves a variable from the name space
     * @param name
     * @return
     */
    public IDataElement get(String name) throws DataSpaceException;

    public boolean hasVariable(String name) throws DataSpaceException;

    /**
     * Removes the element from the namespace and if the element was persistent it is demoted to temporary
     * @param name
     * @return
     */
    public IDataElement remove(String name) throws DataSpaceException;

    /**
     * Removes the value from the name space by identity, not equality
     * @param value
     */
    void remove(IDataElement value) throws DataSpaceException;

    List<String> getAssignedVariableNames();

    void removeDataFrameAndAllVectors(String name) throws DataSpaceException;
}
