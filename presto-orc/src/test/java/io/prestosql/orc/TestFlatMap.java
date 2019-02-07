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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.Iterators.advance;
import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.prestosql.orc.TestFlatMap.ExpectedValuesBuilder.Frequency.ALL;
import static io.prestosql.orc.TestFlatMap.ExpectedValuesBuilder.Frequency.NONE;
import static io.prestosql.orc.TestFlatMap.ExpectedValuesBuilder.Frequency.SOME;
import static io.prestosql.orc.TestingOrcPredicate.createOrcPredicate;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestFlatMap
{
    // TODO: Add tests for timestamp as value type

    private static final TypeManager TYPE_MANAGER = new TypeRegistry();
    private static final int NUM_ROWS = 31_234;

    static {
        // associate TYPE_MANAGER with a function registry
        new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), new FeaturesConfig());
    }

    private static final Type LIST_TYPE = TYPE_MANAGER.getParameterizedType(
            StandardTypes.ARRAY,
            ImmutableList.of(TypeSignatureParameter.of(IntegerType.INTEGER.getTypeSignature())));
    private static final Type MAP_TYPE = TYPE_MANAGER.getParameterizedType(
            StandardTypes.MAP,
            ImmutableList.of(TypeSignatureParameter.of(VarcharType.VARCHAR.getTypeSignature()), TypeSignatureParameter.of(RealType.REAL.getTypeSignature())));
    private static final Type STRUCT_TYPE = TYPE_MANAGER.getParameterizedType(
            StandardTypes.ROW,
            ImmutableList.of(
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value1", false)), IntegerType.INTEGER.getTypeSignature())),
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value2", false)), IntegerType.INTEGER.getTypeSignature())),
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value3", false)), IntegerType.INTEGER.getTypeSignature()))));

    @Test
    public void testByte()
            throws Exception
    {
        runTest("test_flat_map/flat_map_byte.dwrf",
                TinyintType.TINYINT,
                ExpectedValuesBuilder.get(Integer::byteValue));
    }

    @Test
    public void testByteWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_byte_with_null.dwrf",
                TinyintType.TINYINT,
                ExpectedValuesBuilder.get(Integer::byteValue).setNullValuesFrequency(SOME));
    }

    @Test
    public void testShort()
            throws Exception
    {
        runTest("test_flat_map/flat_map_short.dwrf",
                SmallintType.SMALLINT,
                ExpectedValuesBuilder.get(Integer::shortValue));
    }

    @Test
    public void testInteger()
            throws Exception
    {
        runTest("test_flat_map/flat_map_int.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()));
    }

    @Test
    public void testIntegerWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_int_with_null.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullValuesFrequency(SOME));
    }

    @Test
    public void testLong()
            throws Exception
    {
        runTest("test_flat_map/flat_map_long.dwrf",
                BigintType.BIGINT,
                ExpectedValuesBuilder.get(Integer::longValue));
    }

    @Test
    public void testString()
            throws Exception
    {
        runTest("test_flat_map/flat_map_string.dwrf",
                VarcharType.VARCHAR,
                ExpectedValuesBuilder.get(i -> Integer.toString(i)));
    }

    @Test
    public void testStringWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_string_with_null.dwrf",
                VarcharType.VARCHAR,
                ExpectedValuesBuilder.get(i -> Integer.toString(i)).setNullValuesFrequency(SOME));
    }

    @Test
    public void testBinary()
            throws Exception
    {
        runTest("test_flat_map/flat_map_binary.dwrf",
                VarbinaryType.VARBINARY,
                ExpectedValuesBuilder.get(i -> new SqlVarbinary(Integer.toString(i).getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        runTest("test_flat_map/flat_map_boolean.dwrf",
                IntegerType.INTEGER,
                BooleanType.BOOLEAN,
                ExpectedValuesBuilder.get(Function.identity(), TestFlatMap::intToBoolean));
    }

    @Test
    public void testBooleanWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_boolean_with_null.dwrf",
                IntegerType.INTEGER,
                BooleanType.BOOLEAN,
                ExpectedValuesBuilder.get(Function.identity(), TestFlatMap::intToBoolean).setNullValuesFrequency(SOME));
    }

    @Test
    public void testFloat()
            throws Exception
    {
        runTest("test_flat_map/flat_map_float.dwrf",
                IntegerType.INTEGER,
                RealType.REAL,
                ExpectedValuesBuilder.get(Function.identity(), Float::valueOf));
    }

    @Test
    public void testFloatWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_float_with_null.dwrf",
                IntegerType.INTEGER,
                RealType.REAL,
                ExpectedValuesBuilder.get(Function.identity(), Float::valueOf).setNullValuesFrequency(SOME));
    }

    @Test
    public void testDouble()
            throws Exception
    {
        runTest("test_flat_map/flat_map_double.dwrf",
                IntegerType.INTEGER,
                DoubleType.DOUBLE,
                ExpectedValuesBuilder.get(Function.identity(), Double::valueOf));
    }

    @Test
    public void testDoubleWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_double_with_null.dwrf",
                IntegerType.INTEGER,
                DoubleType.DOUBLE,
                ExpectedValuesBuilder.get(Function.identity(), Double::valueOf).setNullValuesFrequency(SOME));
    }

    @Test
    public void testList()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_list.dwrf",
                IntegerType.INTEGER,
                LIST_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestFlatMap::intToList));
    }

    @Test
    public void testListWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_list_with_null.dwrf",
                IntegerType.INTEGER,
                LIST_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestFlatMap::intToList).setNullValuesFrequency(SOME));
    }

    @Test
    public void testMap()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_map.dwrf",
                IntegerType.INTEGER,
                MAP_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestFlatMap::intToMap));
    }

    @Test
    public void testMapWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_map_with_null.dwrf",
                IntegerType.INTEGER,
                MAP_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestFlatMap::intToMap).setNullValuesFrequency(SOME));
    }

    @Test
    public void testStruct()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_struct.dwrf",
                IntegerType.INTEGER,
                STRUCT_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestFlatMap::intToList));
    }

    @Test
    public void testStructWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_struct_with_null.dwrf",
                IntegerType.INTEGER,
                STRUCT_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestFlatMap::intToList).setNullValuesFrequency(SOME));
    }

    @Test
    public void testWithNulls()
            throws Exception
    {
        // A test case where some of the flat maps are null
        runTest(
                "test_flat_map/flat_map_some_null_maps.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullRowsFrequency(SOME));
    }

    @Test
    public void testWithAllNulls()
            throws Exception
    {
        // A test case where every flat map is null
        runTest(
                "test_flat_map/flat_map_all_null_maps.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullRowsFrequency(ALL));
    }

    @Test
    public void testWithEmptyMaps()
            throws Exception
    {
        // A test case where some of the flat maps are empty
        runTest(
                "test_flat_map/flat_map_some_empty_maps.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setEmptyMapsFrequency(SOME));
    }

    @Test
    public void testWithAllMaps()
            throws Exception
    {
        // A test case where all of the flat maps are empty
        runTest(
                "test_flat_map/flat_map_all_empty_maps.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setEmptyMapsFrequency(ALL));
    }

    private <K, V> void runTest(String testOrcFileName, Type type, ExpectedValuesBuilder<K, V> expectedValuesBuilder)
            throws Exception
    {
        runTest(testOrcFileName, type, type, expectedValuesBuilder);
    }

    private <K, V> void runTest(String testOrcFileName, Type keyType, Type valueType, ExpectedValuesBuilder<K, V> expectedValuesBuilder)
            throws Exception
    {
        List<Map<K, V>> expectedValues = expectedValuesBuilder.build();

        runTest(testOrcFileName, keyType, valueType, expectedValues, false, false);
        runTest(testOrcFileName, keyType, valueType, expectedValues, true, false);
        runTest(testOrcFileName, keyType, valueType, expectedValues, false, true);
    }

    private <K, V> void runTest(String testOrcFileName, Type keyType, Type valueType, List<Map<K, V>> expectedValues, boolean skipFirstBatch, boolean skipFirstStripe)
            throws Exception
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(
                new File(getResource(testOrcFileName).getFile()),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);
        OrcReader orcReader = new OrcReader(
                orcDataSource,
                OrcEncoding.DWRF,
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, DataSize.Unit.MEGABYTE));
        Type mapType = TYPE_MANAGER.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(
                        TypeSignatureParameter.of(keyType.getTypeSignature()),
                        TypeSignatureParameter.of(valueType.getTypeSignature())));

        try (OrcRecordReader recordReader = orcReader.createRecordReader(ImmutableMap.of(0, mapType), createOrcPredicate(mapType, expectedValues, OrcTester.Format.DWRF, true), HIVE_STORAGE_TIME_ZONE, newSimpleAggregatedMemoryContext(), 1024)) {
            Iterator<?> expectedValuesIterator = expectedValues.iterator();

            boolean isFirst = true;
            int rowsProcessed = 0;
            for (int batchSize = toIntExact(recordReader.nextBatch()); batchSize >= 0; batchSize = toIntExact(recordReader.nextBatch())) {
                if (skipFirstStripe && rowsProcessed < 10_000) {
                    assertEquals(advance(expectedValuesIterator, batchSize), batchSize);
                }
                else if (skipFirstBatch && isFirst) {
                    assertEquals(advance(expectedValuesIterator, batchSize), batchSize);
                    isFirst = false;
                }
                else {
                    Block block = recordReader.readBlock(mapType, 0);

                    for (int position = 0; position < block.getPositionCount(); position++) {
                        assertEquals(mapType.getObjectValue(SESSION, block, position), expectedValuesIterator.next());
                    }
                }

                assertEquals(recordReader.getReaderPosition(), rowsProcessed);
                assertEquals(recordReader.getFilePosition(), rowsProcessed);
                rowsProcessed += batchSize;
            }

            assertFalse(expectedValuesIterator.hasNext());
            assertEquals(recordReader.getReaderPosition(), rowsProcessed);
            assertEquals(recordReader.getFilePosition(), rowsProcessed);
        }
    }

    private static boolean intToBoolean(int i)
    {
        return i % 2 == 0;
    }

    private static SqlTimestamp intToTimestamp(int i)
    {
        return new SqlTimestamp(i, TimeZoneKey.UTC_KEY);
    }

    private static List<Integer> intToList(int i)
    {
        return ImmutableList.of(i * 3, i * 3 + 1, i * 3 + 2);
    }

    private static Map<String, Float> intToMap(int i)
    {
        return ImmutableMap.of(Integer.toString(i * 3), (float) (i * 3), Integer.toString(i * 3 + 1), (float) (i * 3 + 1), Integer.toString(i * 3 + 2), (float) (i * 3 + 2));
    }

    static class ExpectedValuesBuilder<K, V>
    {
        enum Frequency
        {
            NONE,
            SOME,
            ALL
        }

        private final Function<Integer, K> keyConverter;
        private final Function<Integer, V> valueConverter;
        private Frequency nullValuesFrequency = NONE;
        private Frequency nullRowsFrequency = NONE;
        private Frequency emptyMapsFrequency = NONE;

        private ExpectedValuesBuilder(Function<Integer, K> keyConverter, Function<Integer, V> valueConverter)
        {
            this.keyConverter = keyConverter;
            this.valueConverter = valueConverter;
        }

        public static <T> ExpectedValuesBuilder<T, T> get(Function<Integer, T> converter)
        {
            return new ExpectedValuesBuilder<>(converter, converter);
        }

        public static <K, V> ExpectedValuesBuilder<K, V> get(Function<Integer, K> keyConverter, Function<Integer, V> valueConverter)
        {
            return new ExpectedValuesBuilder<>(keyConverter, valueConverter);
        }

        public ExpectedValuesBuilder<K, V> setNullValuesFrequency(Frequency frequency)
        {
            this.nullValuesFrequency = frequency;

            return this;
        }

        public ExpectedValuesBuilder<K, V> setNullRowsFrequency(Frequency frequency)
        {
            this.nullRowsFrequency = frequency;

            return this;
        }

        public ExpectedValuesBuilder<K, V> setEmptyMapsFrequency(Frequency frequency)
        {
            this.emptyMapsFrequency = frequency;

            return this;
        }

        public List<Map<K, V>> build()
        {
            List<Map<K, V>> result = new ArrayList<>(NUM_ROWS);

            for (int i = 0; i < NUM_ROWS; ++i) {
                if (passesFrequencyCheck(nullRowsFrequency, i)) {
                    result.add((Map<K, V>) null);
                }
                else if (passesFrequencyCheck(emptyMapsFrequency, i)) {
                    result.add(Collections.emptyMap());
                }
                else {
                    Map<K, V> row = new HashMap<>();
                    row.put(keyConverter.apply(i * 3 % 32), passesFrequencyCheck(nullValuesFrequency, i) ? null : valueConverter.apply(i * 3 % 32));
                    row.put(keyConverter.apply((i * 3 + 1) % 32), valueConverter.apply((i * 3 + 1) % 32));
                    row.put(keyConverter.apply((i * 3 + 2) % 32), valueConverter.apply((i * 3 + 2) % 32));
                    result.add(row);
                }
            }

            return result;
        }

        private boolean passesFrequencyCheck(Frequency frequency, int i)
        {
            switch (frequency) {
                case NONE:
                    return false;
                case ALL:
                    return true;
                case SOME:
                    return i % 5 == 0;
                default:
                    throw new IllegalArgumentException("Got unexpected Frequency: " + frequency);
            }
        }
    }
}
