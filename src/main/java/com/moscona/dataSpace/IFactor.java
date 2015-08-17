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

import java.util.List;

/**
 * Created: Dec 8, 2010 2:24:00 PM
 * By: Arnon Moscona
 * A factor is not strictly a data element in the sense that there are no vectors of it, but it is part of a data space.
 * It is distinguished from "regular" small data element by virtue of not being a scalar.
 */
public interface IFactor<T extends IScalar> extends IDataElement {
    List<T> getValues();

    int intValue(T value) throws DataSpaceException;

    T valueOf(int mappedValue) throws DataSpaceException;

    String getName();
}
