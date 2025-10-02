/*
 * Copyright 2025 Yelp Inc.
 *
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
package com.yelp.nrtsearch.plugins.ingestion.paimon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.function.Consumer;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.*;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ModuloEqualTest {

  @Mock private DataType mockDataType;

  @Mock private FunctionVisitor<String> mockVisitor;

  @Mock private FieldRef mockFieldRef;

  private ModuloEqual moduloEqual;
  private String commitUser;
  private Path tablePath;

  private static final RowType ROW_TYPE =
      RowType.builder()
          .field("id", DataTypes.INT())
          .field("index_column", DataTypes.STRING())
          .field("index_column2", DataTypes.INT())
          .field("index_column3", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
          .build();

  @Before
  public void setUp() throws Exception {
    // Test with modulus=10, remainder=3 (field % 10 == 3)
    moduloEqual = new ModuloEqual(10, 3);
    commitUser = UUID.randomUUID().toString();
    tablePath = new Path("/tmp/paimon-test-" + UUID.randomUUID().toString());
  }

  // ============================================================================
  // test(DataType, Object field, Object literal) tests
  // ============================================================================

  @Test
  public void testTest_IntegerField_MatchingRemainder() {
    // 13 % 10 == 3, should return true
    assertTrue(moduloEqual.test(mockDataType, 13, Collections.emptyList()));
    assertTrue(moduloEqual.test(mockDataType, 23, Collections.emptyList()));
    assertTrue(moduloEqual.test(mockDataType, 3, Collections.emptyList()));
  }

  @Test
  public void testTest_IntegerField_NonMatchingRemainder() {
    // 14 % 10 == 4, should return false
    assertFalse(moduloEqual.test(mockDataType, 14, Collections.emptyList()));
    assertFalse(moduloEqual.test(mockDataType, 25, Collections.emptyList()));
    assertFalse(moduloEqual.test(mockDataType, 0, Collections.emptyList()));
  }

  @Test
  public void testTest_LongField_MatchingRemainder() {
    assertTrue(moduloEqual.test(mockDataType, 13L, Collections.emptyList()));
    assertTrue(moduloEqual.test(mockDataType, 1003L, Collections.emptyList()));
  }

  @Test
  public void testTest_LongField_NonMatchingRemainder() {
    assertFalse(moduloEqual.test(mockDataType, 14L, Collections.emptyList()));
    assertFalse(moduloEqual.test(mockDataType, 1004L, Collections.emptyList()));
  }

  @Test
  public void testTest_DoubleField_MatchingRemainder() {
    // Double values should be converted to long
    assertTrue(moduloEqual.test(mockDataType, 13.7, Collections.emptyList()));
    assertTrue(moduloEqual.test(mockDataType, 23.0, Collections.emptyList()));
  }

  @Test
  public void testTest_DoubleField_NonMatchingRemainder() {
    assertFalse(moduloEqual.test(mockDataType, 14.9, Collections.emptyList()));
    assertFalse(moduloEqual.test(mockDataType, 25.1, Collections.emptyList()));
  }

  @Test
  public void testTest_StringField_ValidNumber_MatchingRemainder() {
    assertTrue(moduloEqual.test(mockDataType, "13", Collections.emptyList()));
    assertTrue(moduloEqual.test(mockDataType, "23", Collections.emptyList()));
  }

  @Test
  public void testTest_StringField_ValidNumber_NonMatchingRemainder() {
    assertFalse(moduloEqual.test(mockDataType, "14", Collections.emptyList()));
    assertFalse(moduloEqual.test(mockDataType, "25", Collections.emptyList()));
  }

  @Test
  public void testTest_StringField_InvalidNumber() {
    // Invalid number string should return false
    assertFalse(moduloEqual.test(mockDataType, "not-a-number", Collections.emptyList()));
    assertFalse(moduloEqual.test(mockDataType, "abc123", Collections.emptyList()));
    assertFalse(moduloEqual.test(mockDataType, "", Collections.emptyList()));
  }

  @Test
  public void testTest_NullField() {
    // Null field should return false
    assertFalse(moduloEqual.test(mockDataType, null, Collections.emptyList()));
  }

  @Test
  public void testTest_ZeroValue() {
    // Test edge case: 0 % 10 == 0
    ModuloEqual zeroRemainder = new ModuloEqual(10, 0);
    assertTrue(zeroRemainder.test(mockDataType, 0, Collections.emptyList()));
    assertTrue(zeroRemainder.test(mockDataType, 10, Collections.emptyList()));
    assertTrue(zeroRemainder.test(mockDataType, 20, Collections.emptyList()));

    assertFalse(zeroRemainder.test(mockDataType, 1, Collections.emptyList()));
    assertFalse(zeroRemainder.test(mockDataType, 9, Collections.emptyList()));
  }

  @Test
  public void testTest_NegativeValues() {
    // Test negative values: -7 % 10 should work correctly in Java
    // Java: -7 % 10 = -7, but we want mathematical modulo
    ModuloEqual sevenRemainder = new ModuloEqual(10, 7);

    // In Java: -3 % 10 = -3, but mathematically we want 7
    // For now, testing Java's behavior - may need adjustment based on requirements
    assertFalse(
        sevenRemainder.test(mockDataType, -3, Collections.emptyList())); // -3 % 10 = -3, not 7
    assertTrue(sevenRemainder.test(mockDataType, 7, Collections.emptyList()));
    assertTrue(sevenRemainder.test(mockDataType, 17, Collections.emptyList()));
  }

  // ============================================================================
  // test(DataType, long rowCount, Object min, Object max, Long nullCount, Object literal) tests
  // ============================================================================

  @Test
  public void testTest_StatisticalPruning_AlwaysReturnsTrue() {
    // Statistical pruning should conservatively return true to avoid false negatives
    assertTrue(moduloEqual.test(mockDataType, 1000L, 0, 100, 0L, Collections.emptyList()));
    assertTrue(moduloEqual.test(mockDataType, 1000L, 50, 60, 10L, Collections.emptyList()));
    assertTrue(moduloEqual.test(mockDataType, 1000L, null, null, null, Collections.emptyList()));
  }

  // ============================================================================
  // negate() tests
  // ============================================================================

  @Test
  public void testNegate_ReturnsEmpty() {
    // ModuloEqual doesn't have an easy negation
    Optional<LeafFunction> negated = moduloEqual.negate();
    assertFalse(negated.isPresent());
  }

  // ============================================================================
  // visit() tests
  // ============================================================================

  @Test
  public void testVisit_ThrowsUnsupportedOperationException() {
    List<Object> literals = Arrays.asList(999); // Should be ignored

    // Create a Boolean visitor mock
    @SuppressWarnings("unchecked")
    FunctionVisitor<Boolean> booleanVisitor = mock(FunctionVisitor.class);

    // ModuloEqual cannot be pushed down to file-level filtering, so visit() throws
    // UnsupportedOperationException to force row-level filtering via executeFilter()
    assertThrows(
        UnsupportedOperationException.class,
        () -> moduloEqual.visit(booleanVisitor, mockFieldRef, literals));
    // Should NOT call any visitor methods since we throw immediately
    verifyNoInteractions(booleanVisitor);
  }

  @Test
  public void testVisit_AlwaysThrowsUnsupportedOperationException() {
    List<Object> literals = Arrays.asList(999);

    // Test with different visitor types - should always throw UnsupportedOperationException
    @SuppressWarnings("unchecked")
    FunctionVisitor<String> stringVisitor = mock(FunctionVisitor.class);

    // ModuloEqual cannot be pushed down, regardless of visitor type
    assertThrows(
        UnsupportedOperationException.class,
        () -> moduloEqual.visit(stringVisitor, mockFieldRef, literals));

    // Should NOT call any visitor methods since we throw immediately
    verifyNoInteractions(stringVisitor);
  }

  // ============================================================================
  // Edge case and boundary tests
  // ============================================================================

  @Test
  public void testModuloEqual_DifferentModulusAndRemainder() {
    // Test with different modulus/remainder combinations
    ModuloEqual mod30rem7 = new ModuloEqual(30, 7);

    assertTrue(mod30rem7.test(mockDataType, 7, Collections.emptyList())); // 7 % 30 = 7
    assertTrue(mod30rem7.test(mockDataType, 37, Collections.emptyList())); // 37 % 30 = 7
    assertTrue(mod30rem7.test(mockDataType, 67, Collections.emptyList())); // 67 % 30 = 7

    assertFalse(mod30rem7.test(mockDataType, 8, Collections.emptyList())); // 8 % 30 = 8
    assertFalse(mod30rem7.test(mockDataType, 38, Collections.emptyList())); // 38 % 30 = 8
  }

  @Test
  public void testModuloEqual_LargeNumbers() {
    // Test with large numbers
    ModuloEqual largeMod = new ModuloEqual(1000, 123);

    assertTrue(largeMod.test(mockDataType, 123L, Collections.emptyList()));
    assertTrue(largeMod.test(mockDataType, 1123L, Collections.emptyList()));
    assertTrue(largeMod.test(mockDataType, 999123L, Collections.emptyList()));

    assertFalse(largeMod.test(mockDataType, 124L, Collections.emptyList()));
    assertFalse(largeMod.test(mockDataType, 1124L, Collections.emptyList()));
  }

  @Test
  public void testModuloEqual_ModulusOne() {
    // Special case: modulus 1 - all numbers should match remainder 0
    ModuloEqual modOne = new ModuloEqual(1, 0);

    assertTrue(modOne.test(mockDataType, 0, Collections.emptyList()));
    assertTrue(modOne.test(mockDataType, 1, Collections.emptyList()));
    assertTrue(modOne.test(mockDataType, 999, Collections.emptyList()));
    assertTrue(modOne.test(mockDataType, -5, Collections.emptyList()));
  }

  @Test
  public void testModuloEqualDirectly() {
    ModuloEqual modEqual = new ModuloEqual(10, 3);

    // Test that our function works directly - literals are ignored
    assertTrue(
        "13 % 10 should equal 3", modEqual.test(DataTypes.INT(), 13, Collections.emptyList()));
    assertTrue(
        "23 % 10 should equal 3", modEqual.test(DataTypes.INT(), 23, Collections.emptyList()));
    assertTrue(
        "33 % 10 should equal 3", modEqual.test(DataTypes.INT(), 33, Collections.emptyList()));

    assertFalse(
        "14 % 10 should NOT equal 3", modEqual.test(DataTypes.INT(), 14, Collections.emptyList()));
    assertFalse(
        "25 % 10 should NOT equal 3", modEqual.test(DataTypes.INT(), 25, Collections.emptyList()));
  }

  @Test
  public void testPartitionColFiltering() throws Exception {
    // Create table with simple integer data for testing modulo filtering
    RowType rowType =
        RowType.builder()
            .field("id", DataTypes.INT())
            .field("__internal_partition_id", DataTypes.INT()) // partition field
            .field("name", DataTypes.STRING())
            .build();

    FileStoreTable table =
        createUnawareBucketFileStoreTable(
            rowType, options -> {}, List.of("__internal_partition_id"), List.of("id"));

    StreamTableWrite write = table.newWrite(commitUser);
    StreamTableCommit commit = table.newCommit(commitUser);
    List<CommitMessage> result = new ArrayList<>();

    // Write test data: nrtsearch_partition with different values
    write.write(GenericRow.of(1, 3, BinaryString.fromString("match1"))); // ✓
    write.write(GenericRow.of(2, 3, BinaryString.fromString("match2"))); // ✓
    write.write(GenericRow.of(3, 4, BinaryString.fromString("nomatch1"))); // ✗
    write.write(GenericRow.of(4, 5, BinaryString.fromString("nomatch2"))); // ✗
    write.write(GenericRow.of(5, 3, BinaryString.fromString("match3"))); // ✓

    result.addAll(write.prepareCommit(true, 0));
    commit.commit(0, result);
    result.clear();

    // create partition column filtering
    Predicate pd = new PredicateBuilder(rowType).equal(1, 3);

    // Apply filter and scan
    TableScan.Plan plan = table.newScan().plan();

    // Read filtered results - executeFilter() is CRITICAL for custom predicates that can't be
    // pushed down
    RecordReader<InternalRow> reader =
        table.newRead().withFilter(pd).executeFilter().createReader(plan.splits());

    List<String> results = new ArrayList<>();
    reader.forEachRemaining(
        row -> {
          int nrtsearch_partition = row.getInt(1);
          String name = row.getString(2).toString();
          results.add(name);

          // Verify that only matching rows are returned (nrtsearch_partition == 3)
          assertEquals(
              "Expected nrtsearch_partition == 3, but got "
                  + nrtsearch_partition
                  + " = "
                  + nrtsearch_partition,
              3,
              nrtsearch_partition);
        });

    // Should only return the 3 matching rows
    assertThat(results).containsExactlyInAnyOrder("match1", "match2", "match3");
    reader.close();
  }

  @Test
  public void testModuloEqualRowFiltering() throws Exception {
    // Create table with simple integer data for testing modulo filtering
    RowType rowType =
        RowType.builder()
            .field("id", DataTypes.INT())
            .field("photo_id", DataTypes.INT()) // This will be our filtering field
            .field("name", DataTypes.STRING())
            .build();

    FileStoreTable table =
        createUnawareBucketFileStoreTable(
            rowType, options -> {}, Collections.emptyList(), Collections.emptyList());

    StreamTableWrite write = table.newWrite(commitUser);
    StreamTableCommit commit = table.newCommit(commitUser);
    List<CommitMessage> result = new ArrayList<>();

    // Write test data: photo_ids with different modulo 10 values
    write.write(GenericRow.of(1, 13, BinaryString.fromString("match1"))); // 13 % 10 = 3 ✓
    write.write(GenericRow.of(2, 23, BinaryString.fromString("match2"))); // 23 % 10 = 3 ✓
    write.write(GenericRow.of(3, 14, BinaryString.fromString("nomatch1"))); // 14 % 10 = 4 ✗
    write.write(GenericRow.of(4, 25, BinaryString.fromString("nomatch2"))); // 25 % 10 = 5 ✗
    write.write(GenericRow.of(5, 33, BinaryString.fromString("match3"))); // 33 % 10 = 3 ✓

    result.addAll(write.prepareCommit(true, 0));
    commit.commit(0, result);
    result.clear();

    // Create ModuloEqual predicate: photo_id % 10 == 3
    ModuloEqual moduloEqual = new ModuloEqual(10, 3);
    Predicate predicate =
        new LeafPredicate(
            moduloEqual,
            DataTypes.INT(),
            1, // field index for photo_id
            "photo_id",
            Collections.singletonList(0));

    // Apply filter and scan
    TableScan.Plan plan = table.newScan().plan();

    // Read filtered results - executeFilter() is CRITICAL for custom predicates that can't be
    // pushed down
    RecordReader<InternalRow> reader =
        table.newRead().withFilter(predicate).executeFilter().createReader(plan.splits());

    List<String> results = new ArrayList<>();
    reader.forEachRemaining(
        row -> {
          int photoId = row.getInt(1);
          String name = row.getString(2).toString();
          results.add(name);

          // Verify that only matching rows are returned (photo_id % 10 == 3)
          assertEquals(
              "Expected photo_id % 10 == 3, but got " + photoId + " % 10 = " + (photoId % 10),
              3,
              photoId % 10);
        });

    // Should only return the 3 matching rows
    assertThat(results).containsExactlyInAnyOrder("match1", "match2", "match3");
    reader.close();
  }

  protected FileStoreTable createUnawareBucketFileStoreTable(
      RowType rowType,
      Consumer<Options> configure,
      List<String> partitionKeys,
      List<String> primaryKeys)
      throws Exception {
    Options conf = new Options();
    conf.set(CoreOptions.PATH, tablePath.toString());
    conf.set(CoreOptions.BUCKET, -1);
    configure.accept(conf);
    TableSchema tableSchema =
        SchemaUtils.forceCommit(
            new SchemaManager(LocalFileIO.create(), tablePath),
            new Schema(rowType.getFields(), partitionKeys, primaryKeys, conf.toMap(), ""));
    return new AppendOnlyFileStoreTable(
        FileIOFinder.find(tablePath), tablePath, tableSchema, CatalogEnvironment.empty());
  }
}
