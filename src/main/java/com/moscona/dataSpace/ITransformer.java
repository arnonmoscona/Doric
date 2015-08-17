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

/**
 * Created: 12/30/10 4:20 PM
 * By: Arnon Moscona
 */
public interface ITransformer<TInput extends IDataElement, TOutput extends IDataElement> {


    /**
     * transform the input data element (vector, data frame or whatever) the the appropriate output (vector, data frame, or whatever)
     * @param input
     * @param queryState
     * @return
     * @throws DataSpaceException
     */
    public TOutput transform(TInput input, IQueryState queryState) throws DataSpaceException;

    /**
     * transform the input data element (vector, data frame or whatever) the the appropriate output (vector, data frame, or whatever)
     * @param input
     * @param selection a bitmap that restricts the transformation to the selected values. Null means all
     * @param queryState
     * @return
     * @throws DataSpaceException
     */
    public TOutput transform(TInput input, IBitMap selection, IQueryState queryState) throws DataSpaceException;
}
