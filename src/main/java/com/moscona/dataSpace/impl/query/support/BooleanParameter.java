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

/**
 * Created: 12/15/10 6:21 PM
 * By: Arnon Moscona
 */
public class BooleanParameter extends AbstractQueryParameter<Boolean> {
    public BooleanParameter(Boolean value, String name, String description) {
        super(ParameterType.BOOLEAN, value, name, description);
    }

    @Override
    public void setValue(String value) throws DataSpaceException {
        validateNotNull(value);
        String v = value.toLowerCase();
        if (v.equals("true") || v.equals("1")) {
            setValue(true);
        }
        else if (v.equals("false") || v.equals("0") || v.equals("-1")) {
            setValue(false);
        }
        else {
            incompatibleValue(value);
        }
    }

    @Override
    public void setValue(long value) throws DataSpaceException {
        if (value==0 || value==-1) {
            setValue(false);
        }
        else if (value==1) {
            setValue(false);
        }
        else {
            incompatibleValue(value);
        }
    }

    @Override
    public void setValue(double value) throws DataSpaceException {
        incompatibleValue("floating values");
    }

    @Override
    public void setValue(boolean value) throws DataSpaceException {
        this.value = value;
    }
}
