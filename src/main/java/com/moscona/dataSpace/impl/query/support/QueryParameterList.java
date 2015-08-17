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
import com.moscona.dataSpace.IQueryParameterList;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created: 12/15/10 6:53 PM
 * By: Arnon Moscona
 */
public class QueryParameterList implements IQueryParameterList {
    ArrayList<IQueryParameter> params;

    protected QueryParameterList() {
        params = new ArrayList<IQueryParameter>();
    }

    public QueryParameterList add(IQueryParameter param) throws DataSpaceException {
        validateNameDoesNotExist(param);
        params.add(param);
        return this;
    }

    private void validateNameDoesNotExist(IQueryParameter param) throws DataSpaceException {
        if (param==null) {
            throw new DataSpaceException("Null parameter argument");
        }
        for (IQueryParameter p:params) {
            if (p.getName().equals(param.getName())) {
                throw new DataSpaceException("Parameter "+param.getName()+" already exists in this list");
            }
        }
    }

    @Override
    public Iterator<IQueryParameter> iterator() {
        return params.iterator();
    }

    @Override
    public IQueryParameterList set(String name, String value) throws DataSpaceException {
        IQueryParameter p = get(name);
        p.setValue(value);
        return this;
    }

    @Override
    public IQueryParameter get(String name) throws DataSpaceException {
        for (IQueryParameter p: params) {
            if (p.getName().equals(name)) {
                return p;
            }
        }
        throw new DataSpaceException("No such parameter found: "+name);
    }

    @Override
    public IQueryParameterList set(String name, double value) throws DataSpaceException {
        IQueryParameter p = get(name);
        p.setValue(value);
        return this;
    }

    @Override
    public IQueryParameterList set(String name, long value) throws DataSpaceException {
        IQueryParameter p = get(name);
        p.setValue(value);
        return this;
    }

    @Override
    public IQueryParameterList set(String name, boolean value) throws DataSpaceException {
        IQueryParameter p = get(name);
        p.setValue(value);
        return this;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public IQueryParameterList set(String name, Collection values) throws DataSpaceException {
        IQueryParameter p = get(name);
        p.setValues(values);
        return this;
    }

    @Override
    public boolean hasNulls() {
        boolean foundNulls = false;
        for (IQueryParameter p: params) {
            foundNulls = foundNulls||p.isNull();
        }
        return foundNulls;
    }

    ArrayList<IQueryParameter> getParams() {
        return params;
    }

    @Override
    public String toString() {
        ArrayList<String> parts = new ArrayList<String>();
        for (IQueryParameter p: params) {
            parts.add(p.getName()+":"+p.getValue());
        }
        return "["+ StringUtils.join(parts,", ")+"]";
    }
}
