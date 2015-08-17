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

import com.moscona.dataSpace.IQueryParameter;
import com.moscona.dataSpace.exceptions.DataSpaceException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created: 12/15/10 6:12 PM
 * By: Arnon Moscona
 */
public abstract class AbstractQueryParameter<T> implements IQueryParameter<T> {
    protected ParameterType type;
    protected T value;
    protected Set<T> valueSet = null;
    protected String name;
    protected String description;

    private AbstractQueryParameter(ParameterType type, String name, String description) {
        this.type=type;
        this.name=name;
        this.description=description;
        value=null;
        valueSet = null;
    }

    protected AbstractQueryParameter(ParameterType type, T value, String name, String description) {
        this(type,name,description);
        this.value=value;
    }

    protected AbstractQueryParameter(ParameterType type, Collection<T> values, String name, String description) {
        this(type,name,description);
        if (values != null) {
            this.valueSet = new HashSet<T>(values);
        }
    }

    @Override
    public ParameterType getType() {
        return type;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value==null;
    }

    protected void validateNotNull(Object value) throws DataSpaceException {
        if (value==null) {
            throw new DataSpaceException(getType().toString()+" cannot take null values");
        }
    }

    protected void incompatibleValue(Object value) throws DataSpaceException {
        throw new DataSpaceException(getType().toString()+" parameters cannot take the value \""+value+"\"");
    }

    @Override
    public void setValues(Collection<T> value) throws DataSpaceException {
        throw new DataSpaceException("Scalar parameters cannot take a set value");
    }

    @Override
    public Set<T> getValueSet() {
        return new HashSet<T>(valueSet);
    }
}
