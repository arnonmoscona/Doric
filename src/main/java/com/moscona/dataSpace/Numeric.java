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
 * Created: Dec 8, 2010 2:40:44 PM
 * By: Arnon Moscona
 */
public class Numeric<T extends Number> extends AbstractDataElement implements IScalar<T> {
    private static final long serialVersionUID = 1480000742312210224L;
    private T value;

    public Numeric(T value) {
        this.value = value;
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public long getLongValue() {
        return value.longValue();
    }

    @Override
    public double getDoubleValue() {
        return value.doubleValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj==null) return false;
        return obj.getClass()==getClass() && ((Numeric)obj).value.equals(value);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getClass().getName()).append(value).toHashCode();
    }

    @Override
    public int compareTo(IScalar<T> o) {
        return CompareToBuilder.reflectionCompare(this, o);
    }

    /**
     * Tries to convert both to long (as long as the argument is some kind of an integral value) and compares a to a
     * long version of this Numeric (as long as this is an integral value)
     * @param obj
     * @return
     * @throws DataSpaceException
     */
    public boolean equalsAsLong(Object obj) throws DataSpaceException {
        if (! isIntegralNumber()) {
            throw new DataSpaceException("can only compare integral types for equality. This is not an integral type");
        }
        long me = asLong();
        if (Numeric.class.isAssignableFrom(obj.getClass())) {
            return me == ((Numeric)obj).asLong();
        }
        if (obj instanceof Long) {
            return me == (Long)obj;
        }
        if (obj instanceof Integer) {
            return me == (Integer)obj;
        }
        if (obj instanceof Short) {
            return me == (Short)obj;
        }
        return false;
    }

    private long asLong() {
        return value.longValue();
    }

    public boolean isIntegralNumber() {
        return (value instanceof Long) ||
                (value instanceof Integer) ||
                (value instanceof  Short);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public long sizeInBytes() {
        if (value==null) {
            return 2L;
        }
        if (value.getClass()==Double.class) {
            return 2L+Double.SIZE/8;
        }
        if (value.getClass()==Float.class) {
            return 2L+Float.SIZE/8;
        }
        if (value.getClass()==Long.class) {
            return 2L+Long.SIZE/8;
        }
        if (value.getClass()==Integer.class) {
            return 2L+Integer.SIZE/8;
        }
        if (value.getClass()==Short.class) {
            return 2L+Short.SIZE/8;
        }
        if (value.getClass()==Byte.class) {
            return 2L+Byte.SIZE/8;
        }
        return 2L;
    }
}
