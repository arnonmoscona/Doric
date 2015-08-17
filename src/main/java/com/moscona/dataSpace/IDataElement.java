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
import com.moscona.dataSpace.persistence.IStorable;

import java.io.Serializable;

/**
 * Created: Dec 8, 2010 2:18:37 PM
 * By: Arnon Moscona
 * All members of a data space are data elements
 */
public interface IDataElement extends IStorable {
    void setNameSpace(INameSpace nameSpace) throws DataSpaceException;
    INameSpace getNameSpace();

    String getDescription();

    IDataElement setDescription(String description);

    boolean isVector();
}
