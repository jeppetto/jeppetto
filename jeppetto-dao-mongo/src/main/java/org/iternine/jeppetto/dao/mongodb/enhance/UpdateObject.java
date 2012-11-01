package org.iternine.jeppetto.dao.mongodb.enhance;


import com.mongodb.DBObject;


public interface UpdateObject {

    DBObject getUpdateClause();

    void setPrefix(String prefix);
}
