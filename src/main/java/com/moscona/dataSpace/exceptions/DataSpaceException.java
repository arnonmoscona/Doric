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

package com.moscona.dataSpace.exceptions;

/**
 * Created: Dec 8, 2010 2:45:48 PM
 * By: Arnon Moscona
 */
public class DataSpaceException extends Exception {
    private static final long serialVersionUID = -5223794496184797050L;

    public DataSpaceException(String message) {
        super(message);
    }

    public DataSpaceException(String message, Throwable cause) {
        super(message, cause);   
    }
}
