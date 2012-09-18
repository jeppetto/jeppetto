package org.iternine.jeppetto.test;


import java.util.List;


public class SummarySimpleObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String id;
    private int intValue;
    private SummaryRelatedObject relatedObject;
    private List<SummaryRelatedObject> relatedObjects;


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }


    public int getIntValue() {
        return intValue;
    }


    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }


    public SummaryRelatedObject getRelatedObject() {
        return relatedObject;
    }


    public void setRelatedObject(SummaryRelatedObject relatedObject) {
        this.relatedObject = relatedObject;
    }


    public List<SummaryRelatedObject> getRelatedObjects() {
        return relatedObjects;
    }


    public void setRelatedObjects(List<SummaryRelatedObject> relatedObjects) {
        this.relatedObjects = relatedObjects;
    }
}
