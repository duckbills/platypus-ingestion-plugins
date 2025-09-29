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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.types.DataType;
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

  @Before
  public void setUp() {
    // Test with modulus=10, remainder=3 (field % 10 == 3)
    moduloEqual = new ModuloEqual(10, 3);
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
  public void testVisit_ReturnsSafeDefault() {
    List<Object> literals = Arrays.asList(999); // Should be ignored

    // Create a Boolean visitor mock
    @SuppressWarnings("unchecked")
    FunctionVisitor<Boolean> booleanVisitor = mock(FunctionVisitor.class);

    // For Boolean visitors, should return Boolean.FALSE (safe default)
    Boolean result = moduloEqual.visit(booleanVisitor, mockFieldRef, literals);

    assertEquals(Boolean.FALSE, result);
    // Should NOT call any visitor methods since we return a safe default
    verifyNoInteractions(booleanVisitor);
  }

  @Test
  public void testVisit_AlwaysReturnsBooleanFalse() {
    List<Object> literals = Arrays.asList(999);

    // Test with different visitor types - should always return Boolean.FALSE
    @SuppressWarnings("unchecked")
    FunctionVisitor<String> stringVisitor = mock(FunctionVisitor.class);

    // Even though the visitor is typed as String, our method will try to return Boolean.FALSE
    // This documents the limitation that custom predicates only work with Boolean visitors
    Object result = moduloEqual.visit(stringVisitor, mockFieldRef, literals);
    assertEquals(
        "Should always return Boolean.FALSE regardless of visitor type", Boolean.FALSE, result);

    // Should NOT call any visitor methods since we return a safe default
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
}
