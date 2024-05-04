package org.bigdatatechcir.warehousev1.flink.java;

import org.apache.paimon.catalog.Catalog;

public class CreateDatabase {
    public static void main(String[] args){
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            catalog.createDatabase("my_db", false);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // do something
        }
    }
}
