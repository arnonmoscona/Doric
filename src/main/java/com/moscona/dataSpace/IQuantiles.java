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

import java.io.Serializable;

/**
 * Created: 12/10/10 4:49 PM
 * By: Arnon Moscona
 * Represents the standard 20 partition quantile set
 */
public interface IQuantiles extends Serializable {
    public enum Form {
        MEDIAN_ONLY, // for vectors or subsets <=5 in length
        FOUR_QUANTILES, // for vectors or subsets 6..20 in length
        FIVE_PERCENTILE_BINS, // for vectors or subsets 21..segment size in length
        ESTIMATED_FIVE_PERCENTILE_BINS // for vectors or subsets longer than segment size
    }

    Double getPercentile(int percentile) throws DataSpaceException;

    Double median();

    Quantiles.Form getForm();
}
