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
 * Created: 12/22/10 3:42 PM
 * By: Arnon Moscona
 */
public class StringSetParameter extends AbstractQueryParameter<String> {
    public StringSetParameter(Collection<String> value, String name, String description) {
        super(ParameterType.STRING_SET, value, name, description);
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
    public void setValues(Collection<String> values) throws DataSpaceException {
        this.valueSet = new HashSet<String>(values);
    }
}
