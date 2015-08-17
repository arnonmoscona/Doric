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
import java.util.Set;

/**
 * Created: Dec 8, 2010 6:21:45 PM
 * By: Arnon Moscona
 */
public interface IQueryParameter<T> {
    /**
     * Query parameter types are a generalized form. The "long" version will be cast to the appropriate concrete type
     * when the query term is applied to a vector. Note that this may cause truncation if your parameter value is
     * out of range. There can be precision loss as well. 
     */
    public enum ParameterType {STRING, LONG, DOUBLE, BOOLEAN, STRING_SET, LONG_SET}

    public ParameterType getType();
    public String getName();
    public String getDescription();
    public void setValue(String value) throws DataSpaceException;
    public void setValue(long value) throws DataSpaceException;
    public void setValue(double value) throws DataSpaceException;
    public void setValue(boolean value) throws DataSpaceException;
    public void setValues(Collection<T> value) throws DataSpaceException;
    public T getValue();
    public Set<T> getValueSet();
    public boolean isNull();
}
