package org.iternine.jeppetto.test;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.ReferenceSet;


public interface SimpleObjectReferencesDAO extends GenericDAO<SimpleObject, String> {

    ReferenceSet<SimpleObject> referenceByAnotherIntValue(int anotherIntValue);
}
