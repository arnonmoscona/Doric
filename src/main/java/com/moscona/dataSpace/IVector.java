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

import java.util.List;
import java.util.function.Consumer;

import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.persistence.PersistenceType;
import com.moscona.exceptions.NotImplementedException;

/**
 * Created: Dec 8, 2010 2:21:36 PM
 * By: Arnon Moscona
 */
public interface IVector<T extends IScalar> extends IDataElement {
    void setMinimumPersistenceType(PersistenceType persistenceType);

    long getFirsSegmentSizeInBytes() throws DataSpaceException;

    public enum BaseType {BOOLEAN, STRING, LONG, INTEGER, SHORT, BYTE, DOUBLE, FLOAT}
    /**
     * Allows us to explicitly identify the base type (of the scalars in the vector)
     * @return
     */
    public BaseType getBaseType();

    public int size();
    public T get(int index) throws ArrayIndexOutOfBoundsException, DataSpaceException;

    /**
     * Creates a new factor value vector based on the unique values of the column and names the factor for it as instructed
     * @param factorName
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     */
    public void factor(String factorName) throws DataSpaceException;

    public void factor(String factorName, IFactor factor) throws DataSpaceException;

    public boolean isFactor();

    public IFactor getFactor() throws DataSpaceException;

    /**
     * Creates a unique list of the vector values and sorts it
     * @return the sorted unique list of values
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     */
    public List<T> getSortedUniqueValues() throws DataSpaceException;

    /**
     * Creates a unique list of the vector values and sorts it
     * @return the sorted unique list of values
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     */
    public List<T> getSortedUniqueValues(int maxUnique) throws DataSpaceException;

    /**
     * Creates a unique list of the filtered vector values and sorts it
     * @param filter
     * @return
     * @throws DataSpaceException
     */
    public List<T> getSortedUniqueValues(IBitMap filter) throws DataSpaceException;

    int countUnique(IBitMap filter) throws DataSpaceException;

    /**
     * Fast transform of the vector to a list
     * @return the contents of the vector as a simple list (careful - this could be very large)
     * @throws DataSpaceException
     */
    public List<T> asList() throws DataSpaceException;

    /**
     * Fast transform of a selection from the vector to a list
     * @param selection
     * @return
     * @throws DataSpaceException
     */
    public List<T> select(IBitMap selection) throws DataSpaceException;

    public IBitMap select(IQueryTerm<T> query, IQueryParameterList parameters, IQueryState queryState) throws DataSpaceException;

    /**
     * Given a bit map of the same length as the vector, produces a vector matching only the true entries in the bit map
     * @param bitmap
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     */
    public IVector<T> subset(IBitMap bitmap) throws DataSpaceException;

    /**
     * A convenience shortcut for vector.subset(query.apply(params,vector,queryState))
     * @param query
     * @param params
     * @param queryState
     * @return
     */
    public IVector<T> subset(IQueryTerm<T> query, IQueryParameterList params, IQueryState queryState) throws DataSpaceException;

    public IVectorIterator<T> iterator() throws DataSpaceException;

    public IVectorIterator<T> iterator(IBitMap result);

    /**
     * After a vector is sealed its overall stats (descriptive+quantiles) are available
     * @return
     * @throws DataSpaceException
     */
    public IVectorStats<T> getStats() throws DataSpaceException;

    public boolean isNumeric();

    IDescriptiveStats<?> getDescriptiveStats(IBitMap selection) throws DataSpaceException, NotImplementedException;

    @Override
    public long sizeInBytes() throws DataSpaceException;

    public DataSpace getDataSpace();

    /**
     * Creates a new copy of this vector in the target data space, which may be different than the one associated
     * with the vector.
     * @param dataSpace
     * @return
     * @throws DataSpaceException
     */
    public IVector<T> copyTo(DataSpace dataSpace) throws DataSpaceException;
    public boolean isReadyToQuery();

    /**
     * An advisory metadata specifying that the vector is sorted.
     * This is a contract with the creator of the vector. The code does not verify that the claim is true.
     * @return
     */
    public boolean isSorted();

    /**
     * Can be called before the vector is sealed to advise users that the vector is sorted. the claim is not verified.
     * It is a promise to users, which can then be used to assess query efficiency apriori.
     * @param isSorted
     * @throws DataSpaceException
     */
    public IVector<T> setSorted(boolean isSorted) throws DataSpaceException;

    /**
     * Efficiently determines whether the vector has more than one value
     * @return
     */
    public boolean hasMoreThanOneValue() throws DataSpaceException;

    /**
     * Performs an internal loop over the vector elements, applying the action to each. Stops at the end
     * or when the action throws any kind of exception.
     *
     * Replaces IVectorIterator as the preferred way to loops over vector elements.
     * An internal loop model and continuation avoids some of the locks used in the previous version of
     * vector iteration. By forcing iteration via this method rather than supporting the more general Iterable
     * interface we force clients to consider single pass processing and manage to avoid locks.
     * You can still do external looping with a for loop and get(i), but be forewarned that this is both
     * inefficient and prone to locking issues.
     *
     * @param action
     */
    public void forEach(Consumer<? super T> action) throws DataSpaceException;

    /**
     * Performs an internal loop over the vector elements, applying the action to each element
     * selected by the bitmap. Stops at the end (last element selected by the bitmap)
     * or when the action throws any kind of exception.
     *
     * This is the recommended and efficient way to iterate over selected elements
     * @param bitmap the selection bitmap
     * @param action the action to apply to each element
     */
    public void forEach(IBitMap bitmap, Consumer<? super T> action);
}
