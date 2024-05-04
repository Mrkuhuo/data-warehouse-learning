package org.bigdatatechcir.warehousev1.flink.java;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;

public class CreateCatalog {
    public static Catalog createFilesystemCatalog(){
        CatalogContext context = CatalogContext.create(new Path("/opt/software/paimon_catelog"));
        return CatalogFactory.createCatalog(context);
    }
}
