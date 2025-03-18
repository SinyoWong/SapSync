package common;

import com.alibaba.fastjson2.JSONArray;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import java.util.Locale;

public class GetTable {

    public static Table getTable(String os,String databaseName,String tableName,JSONArray rowColumn) throws Catalog.TableNotExistException {
//        Identifier identifier = Identifier.create("ods_wj", "my_table");
//        Identifier identifier = Identifier.create("my_db", "my_table");
        Identifier identifier = Identifier.create(databaseName, tableName);
        Catalog catalog;
        if (os.equals("Windows")) {
            catalog = CreateCatalog.createFilesystemCatalog();
        } else {
            catalog = CreateCatalog.createHiveCatalog();
        }

        try {
            boolean databaseExists = catalog.databaseExists(databaseName);
            if (!databaseExists) {
                catalog.createDatabase(databaseName,false);
            }

            boolean exists = catalog.tableExists(identifier);
            if (exists) {
                if (os.equals("Windows")) {
                    catalog.dropTable(identifier, false);
                } else {
                    return catalog.getTable(identifier);
                }
            }
            Schema.Builder schemaBuilder = Schema.newBuilder();
//            schemaBuilder.primaryKey("batch_num", "transaction_id", "id");
//            schemaBuilder.partitionKeys("BATCH_NUM"); // 分区字段
            for (Object o : rowColumn) {
                String cN = (String) o;
                schemaBuilder.column(cN.toLowerCase(Locale.ROOT), DataTypes.STRING());
            }
            schemaBuilder.column("batch_num", DataTypes.BIGINT());
            schemaBuilder.column("transaction_id", DataTypes.BIGINT());
//            schemaBuilder.column("id", DataTypes.STRING());
//            schemaBuilder.column("types", DataTypes.STRING());
//            schemaBuilder.column("ts", DataTypes.STRING());

            Schema schema = schemaBuilder.build();
            catalog.createTable(identifier, schema, false);

        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        } catch (Catalog.TableAlreadyExistException | Catalog.DatabaseNotExistException e) {
            e.printStackTrace();
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new RuntimeException(e);
        }
        return catalog.getTable(identifier);
    }
}
