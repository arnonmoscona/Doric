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
import com.moscona.dataSpace.impl.AbstractVector;
import com.moscona.dataSpace.persistence.IDataStore;
import com.moscona.dataSpace.persistence.PersistenceType;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Created: 12/15/10 11:03 AM
 * By: Arnon Moscona
 */
public class DataBundle implements INameSpace, Serializable {
    private static final long serialVersionUID = 8270307768774394993L;
    private DataSpace dataSpace;
    private PersistenceType defaultPersistenceType;
    private ConcurrentHashMap<String, IDataElement> data;
    private Pattern namePattern;
    private int anonymousCounter;
    private String name;
    private boolean isTemporary=false;
    private boolean isPersistent=false;

    public DataBundle(DataSpace dataSpace, PersistenceType defaultPersistenceType, String name) {
        this.dataSpace = dataSpace;
        this.defaultPersistenceType = defaultPersistenceType;
        this.name = name;
        data = new ConcurrentHashMap<String,IDataElement>();
        namePattern = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]+");
        anonymousCounter = 1;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public PersistenceType getDefaultPersistenceType() {
        return defaultPersistenceType;
    }

    @Override
    public IDataStore getDataStore() {
        return dataSpace.getDataStore();
    }

    /**
     * validates the naming convention and that there is no other variable of this name in the name space
     *
     * @param candidateName
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    public String validateName(String candidateName) throws DataSpaceException {
        if (! namePattern.matcher(candidateName).matches()) {
            throw new DataSpaceException("The name \""+candidateName+"\" is not a valid variable name. Must match /"+namePattern+"/");
        }
        return candidateName;
    }

    public Set<String> keySet() {
        return data.keySet();
    }

    /**
     * Adds the data element to the name space and promotes it to the default persistence type if it was in a lower persistence type
     *
     * @param name
     * @param value
     */
    @Override
    public synchronized void assign(String name, IDataElement value) throws DataSpaceException {
        validateSameDataStore(value);
        dataSpace.notifyNameSpaceChangeStart(this);
        enforceSealed(value);
        try {
            validateName(name);
            if (data.containsKey(name)) {
                throw new DataSpaceException("There is already a value named \""+name+"\" in this name space. ("+this.name+")");
            }
            if (value.getNameSpace() != null) {
                INameSpace originalNameSpace = value.getNameSpace();
                if (originalNameSpace == this) {
                    remove(value);
                }
                else {
                    // remove it from the previously assigned name space
                    originalNameSpace.remove(value);
                    dataSpace.finishVariableMove(value,(DataBundle)originalNameSpace,this);
                }
            }
            else {
                dataSpace.finishVariableMove(value,null,this);
            }
            data.put(name,value);
            value.setNameSpace(this);
            if (DataFrame.class.isAssignableFrom(value.getClass())) {
                enforceVectorMembershipRules(((DataFrame) value));
            }
            dataSpace.notifyNameSpaceChangeFinish(this);
        }
        catch (DataSpaceException e) {
            dataSpace.notifyNameSpaceChangeFailed(this,e);
            throw e;
        }
    }

    private void enforceSealed(IDataElement value) throws DataSpaceException {
        if (value.isVector() && ! ((AbstractVector)value).isSealed()) {
            throw new DataSpaceException("Attempt to operate on an unsealed vector");
        }
    }

    private void validateSameDataStore(IDataElement value) throws DataSpaceException {
        if (IVector.class.isAssignableFrom(value.getClass())) {
            if (dataSpace.getDataStore() != ((IVector)value).getDataSpace().getDataStore()) {
                throw new DataSpaceException("Moving vectors between data stores is not supported");
            }
        }
    }

    /**
     * Makes sure that all vectors of the data frame have at least as high a persistence type as this data bundle does
     * @param df
     */
    protected void enforceVectorMembershipRules(DataFrame df) throws DataSpaceException {
        for (String col: df.getColumnNames()) {
            enforceVectorMembershipRules(df.get(col));
        }
    }

    protected void enforceVectorMembershipRules(IVector vector) throws DataSpaceException {
        PersistenceType vectorPersistenceType = vector.getPersistenceType();
        if (vector.getNameSpace()!=null && vectorPersistenceType!=null && vectorPersistenceType.compareTo(defaultPersistenceType) >= 0) {
            return; // this vector is OK
        }
        add(vector);
        vector.setMinimumPersistenceType(defaultPersistenceType);
    }

    public synchronized void assignAll(Map<String,IDataElement> values) throws DataSpaceException {
        dataSpace.notifyNameSpaceChangeStart(this);
        try {
            for (String name: values.keySet()) {
                assign(name, values.get(name));
            }
            dataSpace.notifyNameSpaceChangeFinish(this);
        }
        catch (DataSpaceException e) {
            dataSpace.notifyNameSpaceChangeFailed(this,e);
        }
    }

    public void addAll(List<IDataElement> values) throws DataSpaceException {
        dataSpace.notifyNameSpaceChangeStart(this);
        try {
            for (IDataElement element: values) {
                add(element);
            }
            dataSpace.notifyNameSpaceChangeFinish(this);
        }
        catch (DataSpaceException e) {
            dataSpace.notifyNameSpaceChangeFailed(this,e);
        }
    }

    /**
     * Adds a value without assigning it a name. It will actually end up with a name, but that is unknown to the caller.
     * Really only useful in non-persistent name spaces and therefore persistent values are prohibited.
     * Note that you can get around this, but you don't want to. If you want persistent items it is best to assign them
     * to a persistent name space.
     *
     * @param anonymous
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *          if the data element was persistent
     */
    @Override
    public String add(IDataElement anonymous) throws DataSpaceException {
        String name = "__" + anonymous.getClass().getSimpleName() + "_"+anonymousCounter++;
        assign(name,anonymous);
        return name;
    }

    /**
     * Retrieves a variable from the name space
     *
     * @param name
     * @return
     */
    @Override
    public synchronized IDataElement get(String name) throws DataSpaceException {
        if (! data.containsKey(name)) {
            throw new DataSpaceException("No such variable: \""+name+"\" in this name space. ("+this.name+")");
        }
        return data.get(name);
    }

    @Override
    public synchronized boolean hasVariable(String name) {
        return data.containsKey(name);
    }

    /**
     * Removes the element from the namespace and if the element was persistent it is demoted to temporary
     *
     * @param name
     * @return
     */
    @Override
    public synchronized IDataElement remove(String name) throws DataSpaceException {
        // HOLD (fix before release)  see issue #IT-476 about issues with removal from persistent name space
        dataSpace.notifyNameSpaceChangeStart(this);
        try {
            IDataElement retval = data.get(name);
            if (retval != null) {
                data.remove(name);
            }
            dataSpace.notifyNameSpaceChangeFinish(this);
            return retval;
        }
        catch (Exception e) {
            dataSpace.notifyNameSpaceChangeFailed(this, e);
            return null;
        }
    }

    @Override
    public void removeDataFrameAndAllVectors(String name) throws DataSpaceException {
        // HOLD (fix before release)  see issue #IT-476 about issues with removal from persistent name space
        IDataElement element = get(name);
        if (element==null) {
            return;
        }
        if (! DataFrame.class.isAssignableFrom(element.getClass())) {
            throw new DataSpaceException("The element \""+name+"\" is not a DataFrame. It is a "+element.getClass().getSimpleName());
        }
        DataFrame df = (DataFrame)element;
        dataSpace.notifyNameSpaceChangeStart(this);
        try {
            HashSet<IVector> vectors = new HashSet<IVector>();
            for (String column: df.getColumnNames()) {
                vectors.add(df.get(column));
            }
            remove(df);
            for (IVector vector: vectors) {
                remove(vector);
            }
            dataSpace.notifyNameSpaceChangeFinish(this);
        }
        catch (Exception e) {
            dataSpace.notifyNameSpaceChangeFailed(this, e);
        }
    }

    @Override
    public synchronized void remove(IDataElement value) throws DataSpaceException {
        // HOLD (fix before release)  see issue #IT-476 about issues with removal from persistent name space
        String toRemove = null;
        for (String name: data.keySet()) {
            IDataElement element = data.get(name);
            if (element==value) {
                toRemove = name;
                break;
            }
        }
        if (toRemove!=null) {
            data.remove(toRemove);
        }
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public void setPersistent(boolean persistent) {
        isPersistent = persistent;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public void setTemporary(boolean temporary) {
        isTemporary = temporary;
    }

    @Override
    public synchronized List<String> getAssignedVariableNames() {
        ArrayList<String> retval = new ArrayList<String>();
        for (String name: data.keySet()) {
            if (! name.startsWith("_")) {
                retval.add(name);
            }
        }
        Collections.sort(retval);
        return retval;
    }

    public synchronized void wipeClean() {
        data.clear();
    }
}
