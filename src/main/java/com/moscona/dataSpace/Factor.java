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
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

/**
 * Created: Dec 8, 2010 3:32:22 PM
 * By: Arnon Moscona
 * An instance of Factor defines a named factor type and the mapping between its native value and integers.
 * -- It helps with sorting according to the factor order (rather than the natural order of T)
 * -- It helps with getting information on all possible values of a vector that is based on it
 * -- It can help efficient vector representation by informing the vector system about the number of possible values
 */
public class Factor<T extends IScalar> extends AbstractDataElement implements IFactor<T> {
    private static final long serialVersionUID = -3440086774914632507L;
    private String name;
    private HashMap<T,Integer> factor;
    private HashMap<Integer,T> reverse;
    private ArrayList<Integer> order=null; // if the mapping was provided with non-sequential integers then we cannot use them directly. This is just a cache;

    public Factor(String name, T[] values) throws DataSpaceException {
        init(name);
        setValues(values);
        validate();
    }

    public Factor(String name, List<T> values) throws DataSpaceException {
        init(name);
        if (values==null || values.size()==0) {
            throw new DataSpaceException("The factor has no values!!");
        }
        setValues(values);
        validate();
    }

    public Factor(String name, Map<T,Integer> mapping) throws DataSpaceException {
        init(name);
        setValues(mapping);
        validate();
    }

    public Factor(String name, Set<T> values) throws DataSpaceException {
        init(name);
        if (values==null || values.size()==0) {
            throw new DataSpaceException("The factor has no values!!");
        }
        ArrayList<T> list = new ArrayList<T>(values);
        T first = list.get(0);
        if (first instanceof Comparable) {
            Collections.sort(list, new Comparator<T>() {
                @Override
                @SuppressWarnings({"unchecked"})
                public int compare(T o1, T o2) {
                    return o1.compareTo(o2);
                }
            });
        }
        setValues(list);
        validate();
    }

    private void validate() throws DataSpaceException {
        if (factor.size() != reverse.size()) {
            throw new DataSpaceException("The factor \""+name+"\" is in an invalid state: the forward mapping and " +
                    "reverse mapping are of unequal size (most likely value resulting from duplication)");
        }
        if (factor.size()==0) {
            throw new DataSpaceException("The factor has no values!!");
        }
    }

    private void setValues(Map<T,Integer> mapping) {
        factor.putAll(mapping);
        makeReverseMap();
    }

    private void setValues(T[] values) {
        for (int i=0; i<values.length; i++) {
            factor.put(values[i],i);
        }
        makeReverseMap();
    }

    private void setValues(List<T> values) {
        for (int i=0; i<values.size(); i++) {
            factor.put(values.get(i),i);
        }
        makeReverseMap();
    }

    private ArrayList<Integer> getOrder() {
        if (order == null) {
            order = new ArrayList<Integer>();
            order.addAll(reverse.keySet());
            Collections.sort(order);
        }
        return order;
    }

    @Override
    public List<T> getValues() {
        ArrayList<T> orderedValues = new ArrayList<T>();
        for (int i: getOrder()) {
            orderedValues.add(reverse.get(i));
        }
        return orderedValues;
    }

    private void makeReverseMap() {
        reverse = new HashMap<Integer, T>();
        for (T key: factor.keySet()) {
            reverse.put(factor.get(key),key);
        }
    }

    private void init(String name) {
        this.name = name;
        factor = new HashMap<T,Integer>();
    }

    public boolean isValidValue(T value) {
        return factor.containsKey(value);
    }

    @Override
    public int intValue(T value) throws DataSpaceException {
        validate(value);
        return factor.get(value);
    }

    private void validate(T value) throws DataSpaceException {
        if (! isValidValue(value)) {
            throw new DataSpaceException("The value '"+value+"' is not a valid value for the factor '"+name+"'");
        }
    }

    public String toString(T value) throws DataSpaceException {
        return value.toString();
    }

    @Override
    public T valueOf(int mappedValue) throws DataSpaceException {
        if (! reverse.containsKey(mappedValue)) {
            throw new DataSpaceException("No value of factor '"+name+"' maps to "+mappedValue);
        }
        return reverse.get(mappedValue);
    }

    @Override
    public String getName() {
        return name;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj==null || ! (obj.getClass() == Factor.class)) {
            return false;
        }

        Factor other = (Factor)obj;
        return name.equals(other.name) && factor.equals(other.factor);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().
                append(getClass().getName()).
                append(name).
                append(factor.hashCode()).toHashCode();
    }

    /**
     * Creates a new factor, identical to the original, but with a new name
     * @param newName
     * @return
     */
    public Factor<T> cloneAndRename(String newName) throws DataSpaceException {
        return new Factor<T>(newName, getValues());
    }

    @SuppressWarnings({"unchecked"})
    protected HashMap<T,Integer>getMapping() {
        return (HashMap<T,Integer>)factor.clone();
    }

    protected T reverseMap(Integer key) {
        return reverse.get(key);
    }

    public FactorValue<T> createFactorValue(Integer key) throws DataSpaceException {
        return new FactorValue<T>(this, reverse.get(key));
    }

    @Override
    public long sizeInBytes() {
        // this is a very rough estimate on the high size - and it doesn't matter since we don't make vectors of these
        return (5L+(long)factor.size()*5)*Long.SIZE/8;
    }
}
