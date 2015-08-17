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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Created: Dec 9, 2010 9:15:43 AM
 * By: Arnon Moscona
 */
public interface IQueryParameterList {
    public Iterator<IQueryParameter> iterator();
    public IQueryParameterList set(String name, String value) throws DataSpaceException;
    public IQueryParameterList set(String name, double value) throws DataSpaceException;
    public IQueryParameterList set(String name, long value) throws DataSpaceException;
    public IQueryParameterList set(String name, boolean value) throws DataSpaceException;
    public IQueryParameterList set(String name, Collection values) throws DataSpaceException;
    public boolean hasNulls();

    IQueryParameter get(String name) throws DataSpaceException;
}
