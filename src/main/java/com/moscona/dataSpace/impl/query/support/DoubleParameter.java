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
 * Created: 12/15/10 6:20 PM
 * By: Arnon Moscona
 */
public class DoubleParameter extends AbstractQueryParameter<Double> {
    public DoubleParameter(Double value, String name, String description) {
        super(ParameterType.DOUBLE, value, name, description);
    }

    @Override
    public void setValue(String value) throws DataSpaceException {
        try {
            setValue(Double.parseDouble(value.replaceAll(",","")));
        }
        catch (NumberFormatException e) {
            incompatibleValue(value+" (NumberFormatException)");
        }
    }

    @Override
    public void setValue(long value) throws DataSpaceException {
        setValue((double)value);
    }

    @Override
    public void setValue(double value) throws DataSpaceException {
        this.value = value;
    }

    @Override
    public void setValue(boolean value) throws DataSpaceException {
        incompatibleValue("boolean "+value);
    }
}
