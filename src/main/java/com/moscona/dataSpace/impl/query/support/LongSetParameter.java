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

package com.moscona.dataSpace.impl.query.support;

import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.query.support.AbstractQueryParameter;

import java.util.Collection;
import java.util.HashSet;

/**
 * Created: 12/22/10 3:45 PM
 * By: Arnon Moscona
 */
public class LongSetParameter extends AbstractQueryParameter<Long> {
    public LongSetParameter(Collection<Long> values, String name, String description) {
        super(ParameterType.LONG_SET, values, name, description);
    }

    @Override
    public void setValue(String value) throws DataSpaceException {
        throw new DataSpaceException("Method not supported. use the constructor to set the value");
    }

    @Override
    public void setValue(long value) throws DataSpaceException {
        throw new DataSpaceException("Method not supported. use the constructor to set the value");
    }

    @Override
    public void setValue(double value) throws DataSpaceException {
        throw new DataSpaceException("Method not supported. use the constructor to set the value");
    }

    @Override
    public void setValue(boolean value) throws DataSpaceException {
        throw new DataSpaceException("Method not supported. use the constructor to set the value");
    }

    @Override
    public void setValues(Collection<Long> value) throws DataSpaceException {
        valueSet = new HashSet<Long>(value);
    }
}
