package common;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

public class CreateCatalog {

    public static Catalog createFilesystemCatalog() {
        CatalogContext context = CatalogContext.create(new Path("file:///C:/Sinyo/PaimonTmp"));
        return CatalogFactory.createCatalog(context);
    }


    public static Catalog createHiveCatalog() {
        // Paimon Hive catalog relies on Hive jars
        // You should add hive classpath or hive bundled jar.
        Options options = new Options();
        options.set("warehouse", "hdfs:///data/paimon");
        options.set("metastore", "hive");
        options.set("uri", "thrift://dev-bg-m01:9083");
        options.set("hive-conf-dir", "/etc/hive/3.1.4.0-315/0/");
        options.set("hadoop-conf-dir", "/etc/hadoop/3.1.4.0-315/0/");
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }
}