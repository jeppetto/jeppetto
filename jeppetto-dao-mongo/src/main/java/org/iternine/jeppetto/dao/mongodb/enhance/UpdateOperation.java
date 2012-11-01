package org.iternine.jeppetto.dao.mongodb.enhance;


public enum UpdateOperation {
    $set,
    $unset,
    $pushAll,
    $pullAll,
    $addToSet
}
