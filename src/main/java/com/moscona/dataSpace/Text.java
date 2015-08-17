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
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created: Dec 8, 2010 2:37:20 PM
 * By: Arnon Moscona
 */
public class Text extends AbstractDataElement implements IScalar<String> {
    private static final long serialVersionUID = 7323307361698431891L;
    private String value;

    public Text(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public long getLongValue() throws DataSpaceException {
        try {
            return Long.parseLong(value.replaceAll(",",""));
        }
        catch (NumberFormatException e) {
            throw new DataSpaceException("Failed to parse \""+value+"\" as Long");
        }
    }

    @Override
    public double getDoubleValue() throws DataSpaceException {
        try {
            return Double.parseDouble(value.replaceAll(",",""));
        }
        catch (NumberFormatException e) {
            throw new DataSpaceException("Failed to parse \""+value+"\" as Double");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj==null) return false;
        return obj.getClass()==getClass() && ((Text)obj).value.equals(value);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getClass().getName()).append(value).toHashCode();
    }

    @Override
    public int compareTo(IScalar<String> o) {
        return CompareToBuilder.reflectionCompare(this, o);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public long sizeInBytes() {
        return (long)Long.SIZE/8*2; // reference to string plus the object itself
    }
}
