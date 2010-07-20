/* Copyright 2009-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import oracle.jdbc.OracleDriver;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.*;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * TODO
 *
 * @author Erich Eichinger
 * @since Mar 25, 2010
 */
public class Export
{
    public static void main(String[] args) throws Exception
    {
        DriverManager.registerDriver(new OracleDriver());

        String[] tables = { "TABLE_NAME" };
        String jdbcUrl = "jdbc:oracle:thin:USERNAME/PASSWORD@localhost:1521/ORCL";

        // database import
        importTables(tables, jdbcUrl);
    }

    private static void importTables(String[] tables, String jdbcUrl) {
        for(String table:tables) {
            Connection jdbcConnection = null;
            IDatabaseConnection connection = null;
            try {
                jdbcConnection = DriverManager.getConnection(jdbcUrl);
                connection = createDbUnitConnection(jdbcConnection, "SCHEMA_NAME");

                try {
                    IDataSet dataSet = new FlatXmlDataSetBuilder().build(new FileInputStream(table + ".xml"));
                    DatabaseOperation.CLEAN_INSERT.execute(connection, dataSet);
                } finally {
                    if (connection!=null) connection.close();
                    if (jdbcConnection!=null) jdbcConnection.close();
                }
            } catch (Exception e) {
                System.out.println("Cant import table " + table + ": " + e);
            }
        }
    }

    private static void exportTables(String[] tables, String url) {
        for(String table :tables) {
            Connection jdbcConnection = null;
            IDatabaseConnection connection = null;
            // partial database export
            try {
                try {
                    jdbcConnection = DriverManager.getConnection(url);
                    connection = createDbUnitConnection(jdbcConnection, "COADMIN");

                    QueryDataSet partialDataSet = new QueryDataSet(connection);
//            partialDataSet.addTable(table, "select * from NCRCPT01 where smp_id='EX10' and cou_iso_id='GB'");
                    String qualifiedTableName = connection.getSchema() + "." + table.replace("T01", "V01");
                    partialDataSet.addTable(table, "SELECT * FROM " + qualifiedTableName);
                    FlatXmlDataSet.write(partialDataSet, new FileOutputStream(table + ".xml"));
                    System.out.println("exported table " + table);
                } finally {
                    if (connection!=null) connection.close();
                    if (jdbcConnection!=null) jdbcConnection.close();
                }
            } catch (Exception e) {
                System.out.println("Cant export table " + table + ": " + e);
            }
        }
    }

    private static IDatabaseConnection createDbUnitConnection(Connection jdbcConnection, String schema) throws DatabaseUnitException {
        IDatabaseConnection connection;
        connection = new DatabaseConnection(jdbcConnection, schema);
        connection.getConfig().setProperty(DatabaseConfig.FEATURE_BATCHED_STATEMENTS, true);
        connection.getConfig().setProperty(DatabaseConfig.PROPERTY_BATCH_SIZE, new Integer(500));
        connection.getConfig().setProperty(DatabaseConfig.PROPERTY_FETCH_SIZE, new Integer(500));
//        connection.getConfig().setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, true);
        connection.getConfig().setProperty(DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES, true);
        connection.getConfig().setProperty(DatabaseConfig.PROPERTY_RESULTSET_TABLE_FACTORY, new ForwardOnlyResultSetTableFactory());
        connection.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new DbUnitOracleDataTypeFactory());
        return connection;
    }
}
