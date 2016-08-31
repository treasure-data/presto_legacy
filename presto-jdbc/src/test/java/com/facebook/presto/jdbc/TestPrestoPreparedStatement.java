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

import com.facebook.presto.plugin.blackhole.BlackHolePlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.ColorType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logging;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoPreparedStatement
{
    private static final DateTimeZone ASIA_ORAL_ZONE = DateTimeZone.forID("Asia/Oral");
    private static final GregorianCalendar ASIA_ORAL_CALENDAR = new GregorianCalendar(ASIA_ORAL_ZONE.toTimeZone());
    private static final String TEST_CATALOG = "test_catalog";

    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");

        setupTestTables();
    }

    private void setupTestTables()
            throws SQLException
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertEquals(statement.executeUpdate("CREATE TABLE test_table (x bigint)"), 0);
        }
    }

    @AfterClass
    public void teardown()
    {
        closeQuietly(server);
    }

    @Test
    public void testExecuteQuery()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?")) {
                statement.setNull(1, Types.VARCHAR);
                statement.setBoolean(2, true);
                statement.setShort(3, (short) 3);
                statement.setInt(4, 4);
                statement.setLong(5, 5L);
                statement.setFloat(6, 6f);
                statement.setDouble(7, 7d);
                statement.setBigDecimal(8, BigDecimal.valueOf(8L));
                statement.setString(9, "9'9");
                statement.setDate(10, new Date(10));
                statement.setTime(11, new Time(11));
                statement.setTimestamp(12, new Timestamp(12));
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());

                assertEquals(rs.getObject(1), null);
                assertEquals(rs.getBoolean(2), true);
                assertEquals(rs.getShort(3), (short) 3);
                assertEquals(rs.getInt(4), 4);
                assertEquals(rs.getLong(5), 5L);
                assertEquals(rs.getFloat(6), 6f);
                assertEquals(rs.getDouble(7), 7d);
                assertEquals(rs.getBigDecimal(8), BigDecimal.valueOf(8L));
                assertEquals(rs.getString(9), "9'9");
                assertEquals(rs.getDate(10).toString(), new Date(10).toString());
                assertEquals(rs.getTime(11).toString(), new Time(11).toString());
                assertEquals(rs.getTimestamp(12), new Timestamp(12));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testExecuteUpdate()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_execute_update (" +
                        "c_null boolean, " +
                        "c_boolean boolean, " +
                        "c_integer integer, " +
                        "c_bigint bigint, " +
                        "c_real real, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_date date, " +
                        "c_time time, " +
                        "c_timestamp timestamp" +
                        ")");
            }

            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO test_execute_update VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                statement.setNull(1, Types.BOOLEAN);
                statement.setBoolean(2, true);
                statement.setInt(3, 3);
                statement.setLong(4, 4);
                statement.setFloat(5, 5f);
                statement.setDouble(6, 6d);
                statement.setBigDecimal(7, BigDecimal.valueOf(7L));
                statement.setString(8, "8'8");
                statement.setDate(9, new Date(9));
                statement.setTime(10, new Time(10));
                statement.setTimestamp(11, new Timestamp(11));
                assertEquals(statement.executeUpdate(), 1);
            }

            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT * FROM test_execute_update");
                assertTrue(rs.next());
                assertEquals(null, rs.getObject(1));
                assertEquals(true, rs.getBoolean(2));
                assertEquals(3, rs.getInt(3));
                assertEquals(4f, rs.getFloat(4));
                assertEquals(5d, rs.getDouble(5));
                assertEquals(BigDecimal.valueOf(6L), rs.getBigDecimal(6));
                assertEquals("7'7", rs.getString(7));
                assertEquals(new Date(8).toString(), rs.getDate(8).toString());
                assertEquals(new Time(9).toString(), rs.getTime(9).toString());
                assertEquals(new Timestamp(10), rs.getTimestamp(10));
                assertFalse(rs.next());
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        return createConnection(format("jdbc:presto://%s", server.getAddress()));
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        return createConnection(format("jdbc:presto://%s/%s/%s", server.getAddress(), catalog, schema));
    }

    private Connection createConnection(String url)
            throws SQLException
    {
        return DriverManager.getConnection(url, "test", null);
    }

    static void closeQuietly(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception ignored) {
        }
    }
}
