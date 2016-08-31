/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrestoPreparedStatement
        extends PrestoStatement
        implements PreparedStatement
{
    private final String originalSql;
    private final String queryName;
    private final ClientSession session;
    private final Map<Integer, String> parameters = new HashMap<>();

    PrestoPreparedStatement(PrestoConnection connection, String sql)
            throws SQLException
    {
        super(connection);
        this.originalSql = sql;
        this.queryName = "preparedQuery";
        this.session = ClientSession.withPreparedStatements(connection.createSession(), ImmutableMap.of(queryName, originalSql));
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        try (PrestoStatement statement = (PrestoStatement) getConnection().createStatement()) {
            return statement.executeQuery(session, usingSql());
        }
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        try (PrestoStatement statement = (PrestoStatement) getConnection().createStatement()) {
            return Ints.saturatedCast(statement.executeLargeUpdate(session, usingSql()));
        }
    }

    private String usingSql()
            throws SQLException
    {
        return "EXECUTE " + queryName + " USING " + formatParameters();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLException
    {
        setParameter(parameterIndex, formatCast(formatNullLiteral(), sqlType));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        setParameter(parameterIndex, formatBooleanLiteral(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setByte");
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        setParameter(parameterIndex, formatLongLiteral(x));
    }

    @Override
    public void setInt(int parameterIndex, int x)
            throws SQLException
    {
        setParameter(parameterIndex, formatLongLiteral(x));
    }

    @Override
    public void setLong(int parameterIndex, long x)
            throws SQLException
    {
        setParameter(parameterIndex, formatLongLiteral(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x)
            throws SQLException
    {
        setParameter(parameterIndex, formatCast(formatDoubleLiteral(x), Types.REAL));
    }

    @Override
    public void setDouble(int parameterIndex, double x)
            throws SQLException
    {
        setParameter(parameterIndex, formatDoubleLiteral(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        setParameter(parameterIndex, formatDecimalLiteral(x));
    }

    @Override
    public void setString(int parameterIndex, String x)
            throws SQLException
    {
        setParameter(parameterIndex, formatStringLiteral(x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBytes");
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        // from presto-parser ExpressionFormatter
        setParameter(parameterIndex, formatDateLiteral(PrestoResultSet.DATE_FORMATTER.print(x.getTime())));
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        setParameter(parameterIndex, formatTimeLiteral(PrestoResultSet.TIME_FORMATTER.print(x.getTime())));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException
    {
        setParameter(parameterIndex, formatTimestampLiteral(PrestoResultSet.TIMESTAMP_FORMATTER.print(x.getTime())));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void clearParameters()
            throws SQLException
    {
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        switch (targetSqlType) {
            case Types.NULL:
                setNull(parameterIndex, targetSqlType);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                setShort(parameterIndex, x instanceof Short ? (Short) x : Short.valueOf(String.valueOf(x)));
                break;
            case Types.INTEGER:
                setInt(parameterIndex, x instanceof Integer ? (Integer) x : Integer.valueOf(String.valueOf(x)));
                break;
            case Types.FLOAT:
            case Types.REAL:
                setFloat(parameterIndex, x instanceof Float ? (Float) x : Float.valueOf(String.valueOf(x)));
                break;
            case Types.DOUBLE:
                setDouble(parameterIndex, x instanceof Double ? (Double) x : Double.valueOf(String.valueOf(x)));
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                setBigDecimal(parameterIndex, x instanceof BigDecimal ? (BigDecimal) x : new BigDecimal(String.valueOf(x)));
                break;
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.OTHER:
                setString(parameterIndex, String.valueOf(x));
                break;
            case Types.TIME:
                if (x instanceof Time) {
                    setTime(parameterIndex, (Time) x);
                }
                else if (x instanceof java.util.Date) {
                    setTime(parameterIndex, new Time(((java.util.Date) x).getTime()));
                }
                else {
                    throw new SQLException("Unsupported target SQL type conversion: " + x.getClass().getName() + " to " + targetSqlType);
                }
                break;
            case Types.DATE:
                if (x instanceof Date) {
                    setDate(parameterIndex, (Date) x);
                }
                else if (x instanceof java.util.Date) {
                    setDate(parameterIndex, new Date(((java.util.Date) x).getTime()));
                }
                else {
                    throw new SQLException("Unsupported target SQL type conversion: " + x.getClass().getName() + " to " + targetSqlType);
                }
                break;
            case Types.TIMESTAMP:
                if (x instanceof Timestamp) {
                    setTimestamp(parameterIndex, (Timestamp) x);
                }
                else if (x instanceof java.util.Date) {
                    setTimestamp(parameterIndex, new Timestamp(((java.util.Date) x).getTime()));
                }
                else {
                    throw new SQLException("Unsupported target SQL type conversion: " + x.getClass().getName() + " to " + targetSqlType);
                }
                break;
            default:
                throw new SQLException("Unsupported target SQL type: " + targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException
    {
        if (x == null) {
            setNull(parameterIndex, Types.OTHER);
        }
        else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        }
        else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        }
        else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        }
        else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        }
        else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        }
        else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        }
        else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        }
        else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        }
        else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        }
        else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        }
        else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        }
        else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        }
        else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        }
        else {
            throw new SQLException("Unsupported object type: " + x.getClass().getName());
        }
    }

    @Override
    public boolean execute()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "execute");
    }

    @Override
    public void addBatch()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "addBatch");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getMetaData");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTimestamp");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException
    {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setURL");
    }

    @Override
    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "getParameterMetaData");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setObject");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    private void setParameter(int parameterIndex, String value)
            throws SQLException
    {
        if (parameterIndex < 1) {
            throw new SQLException("Parameter index out of bound: " + (parameterIndex + 1));
        }
        parameters.put(parameterIndex - 1, value);
    }

    private String formatParameters()
            throws SQLException
    {
        List<String> values = new ArrayList<>();
        for (int idx = 0; idx < parameters.size(); ++idx) {
            if (!parameters.containsKey(idx)) {
                throw new SQLException(String.format("No value specified for parameter %d", idx + 1));
            }
            values.add(parameters.get(idx));
        }
        return Joiner.on(", ").join(values);
    }

    // Following definitions are from presto-parser's ExpressionFormatter

    private static String formatNullLiteral()
    {
        return "null";
    }

    private static String formatBooleanLiteral(boolean x)
    {
        return String.valueOf(x);
    }

    private static String formatStringLiteral(String x)
    {
        return "'" + x.replace("'", "''") + "'";
    }

    private static String formatLongLiteral(long x)
    {
        return Long.toString(x);
    }

    private static String formatDoubleLiteral(double x)
    {
        return Double.toString(x);
    }

    private static String formatDecimalLiteral(BigDecimal x)
    {
        return "DECIMAL " + formatStringLiteral(x.toString());
    }

    private static String formatDateLiteral(String x)
    {
        return "DATE " + formatStringLiteral(x);
    }

    private static String formatTimeLiteral(String x)
    {
        return "TIME " + formatStringLiteral(x);
    }

    private static String formatTimestampLiteral(String x)
    {
        return "TIMESTAMP " + formatStringLiteral(x);
    }

    private static String formatCast(String x, int targetSqlType)
            throws SQLException
    {
        switch (targetSqlType) {
            case Types.BOOLEAN:
                return formatCast(x, "BOOLEAN");
            case Types.TINYINT:
                return formatCast(x, "TINYINT");
            case Types.SMALLINT:
                return formatCast(x, "SMALLINT");
            case Types.INTEGER:
                return formatCast(x, "INTEGER");
            case Types.BIGINT:
                return formatCast(x, "BIGINT");
            case Types.FLOAT:
            case Types.REAL:
                return formatCast(x, "REAL");
            case Types.DOUBLE:
                return formatCast(x, "DOUBLE");
            case Types.DECIMAL:
            case Types.NUMERIC:
                return formatCast(x, "DECIMAL");
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return formatStringLiteral(x);
            case Types.TIME:
                return formatTimeLiteral(x);
            case Types.DATE:
                return formatDateLiteral(x);
            case Types.TIMESTAMP:
                return formatTimestampLiteral(x);
            case Types.OTHER:
                return x;
            default:
                throw new SQLException("Unsupported target SQL type: " + targetSqlType);
        }
    }

    private static String formatCast(String x, String prestoType)
    {
        return String.format("CAST(%s AS %s)", x, prestoType);
    }
}
