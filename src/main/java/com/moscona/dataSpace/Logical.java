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


/**
 * Created: Dec 8, 2010 2:31:05 PM
 * By: Arnon Moscona
 * An immutable boolean
 */
public class Logical extends AbstractDataElement implements IScalar<Boolean> {
    private static final long serialVersionUID = 3080959793624133558L;
    private boolean value;

    public Logical(boolean value) {
        this.value = value;
    }

    @Override
    public Boolean getValue() {
        return value;
    }

    @Override
    public long getLongValue() {
        return value ? 1 : 0;
    }

    @Override
    public double getDoubleValue() {
        return value ? 1 : 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj==null) return false;
        return obj.getClass()==getClass() && ((Logical)obj).value == value;
    }

    @Override
    public int hashCode() {
        return Boolean.valueOf(value).hashCode();
    }

    @Override
    public int compareTo(IScalar<Boolean> o) {
        return Boolean.valueOf(value).compareTo(o.getValue());
    }

    @Override
    public String toString() {
        return Boolean.toString(value);
    }

    @Override
    public long sizeInBytes() {
        return (long)Long.SIZE/8+1;
    }
}
