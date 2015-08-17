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
 * Created: Dec 8, 2010 3:43:52 PM
 * By: Arnon Moscona
 * This is a value from a specific Factor<T>. It is not normally used directly for vectors, but can be used for helping
 * sort operations etc.
 */
public class FactorValue<T extends IScalar> extends AbstractDataElement {
    private static final long serialVersionUID = -8895061377062515973L;
    private Factor<T> factor;
    private T value;

    public FactorValue(Factor<T> factor, T value) throws DataSpaceException {
        this.factor = factor;
        this.value = value;
        if (!factor.isValidValue(value)) {
            throw new DataSpaceException("The value '"+value+"' is not allowed for the factor "+factor.getName());  
        }
    }

    public T getValue() {
        return value;
    }

    public int getIntMapping() {
        try {
            return factor.intValue(value);
        }
        catch (DataSpaceException e) {
            return 0; // should never happen - already validated
        }
    }

    public Factor<T> getFactor() {
        return factor;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getClass().getName()).append(value).append(factor.hashCode()).toHashCode();          
    }

    @SuppressWarnings({"unchecked"})
    public int compareTo(Object o) {
        if (factor.equals(((FactorValue<T>)o).getFactor())) {
            return CompareToBuilder.reflectionCompare(this, o);
        }
        throw new ClassCastException("may not compare two factor values of different factors");
    }

    @Override
    public long sizeInBytes() {
        return Long.SIZE / 4; // very rough estimate, but good enough as we do not make vectors directly out of these
    }
}
