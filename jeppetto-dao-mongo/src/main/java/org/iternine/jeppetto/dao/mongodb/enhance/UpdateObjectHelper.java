package org.iternine.jeppetto.dao.mongodb.enhance;


import java.util.List;
import java.util.Map;
import java.util.Set;


public class UpdateObjectHelper {

    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public boolean needsNoConversion(Class clazz) {
        return DBObjectUtil.needsNoConversion(clazz);
    }


    public boolean isAssignableFromList(Class clazz) {
        return List.class.isAssignableFrom(clazz);
    }


    public boolean isAssignableFromMap(Class clazz) {
        return Map.class.isAssignableFrom(clazz);
    }


    public boolean isAssignableFromSet(Class clazz) {
        return Set.class.isAssignableFrom(clazz);
    }
}
