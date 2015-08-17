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
 * Created: 12/9/10 10:40 AM
 * By: Arnon Moscona
 * A value from a factor. Note that factor vectors are not made up of IFactorValue elements and implement IVector<T>.
 * This is to simplify the interface.
 */
public interface IFactorValue<T extends IScalar> extends Comparable {
    public IFactor<T> getFactor();
    public T getValue();
    public int getIntMapping();
}
