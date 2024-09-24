package com.starrocks.connector.jdbc;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.TimeUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static java.lang.Math.max;

/**
 * Created by liujing on 2024/9/24.
 */
public class DamengSchemaResolver extends JDBCSchemaResolver {

    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), dbName + "%", null,
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName + "%", tblName, "%");
    }

    @Override
    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            Type type = convertColumnType(columnSet.getInt("DATA_TYPE"), columnSet.getString("TYPE_NAME"), columnSet.getInt("COLUMN_SIZE"), columnSet.getInt("DECIMAL_DIGITS"));
            String columnName = columnSet.getString("COLUMN_NAME");
            columnName = "\"" + columnName + "\"";
            fullSchema.add(new Column(columnName, type, columnSet.getString("IS_NULLABLE").equals("YES")));
        }
        return fullSchema;
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        PrimitiveType primitiveType;
        switch (dataType) {
            case Types.BIT:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case Types.SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case Types.INTEGER:
                primitiveType = PrimitiveType.INT;
                break;
            case Types.BIGINT:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case Types.REAL:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.NUMERIC:
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case Types.CHAR:
                return ScalarType.createCharType(columnSize);
            case Types.VARCHAR:
                if (typeName.equalsIgnoreCase("varchar")) {
                    return ScalarType.createVarcharType(columnSize);
                } else if (typeName.equalsIgnoreCase("text")) {
                    return ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());
                }
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }
        if (primitiveType != PrimitiveType.DECIMAL32) {
            return ScalarType.createType(primitiveType);
        } else {
            int precision = columnSize + max(-digits, 0);
            if (precision == 0) {
                return ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());
            }
            return ScalarType.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }

    @Override
    public List<Partition> getPartitions(Connection connection, Table table) {
        return Lists.newArrayList(new Partition(table.getName(), TimeUtils.getEpochSeconds()));
    }
}
