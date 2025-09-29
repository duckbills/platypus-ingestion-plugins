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

import java.util.List;
import java.util.Optional;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Paimon predicate function for modulo-based ID sharding. Tests if field % modulus ==
 * remainder (for ID sharding like photo_id % 30 == 23).
 */
public class ModuloEqual extends LeafFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(ModuloEqual.class);

  private final int modulus;
  private final int remainder;

  public ModuloEqual(int modulus, int remainder) {
    this.modulus = modulus;
    this.remainder = remainder;
  }

  @Override
  public boolean test(DataType type, Object field, List<Object> literals) {
    if (field == null) {
      LOGGER.debug("ModuloEqual.test() called with null field, returning false");
      return false;
    }

    // Convert field to long for modulo operation
    long fieldValue;
    if (field instanceof Number) {
      fieldValue = ((Number) field).longValue();
    } else {
      try {
        fieldValue = Long.parseLong(field.toString());
      } catch (NumberFormatException e) {
        LOGGER.debug(
            "ModuloEqual.test() failed to parse field '{}' as number, returning false", field);
        return false;
      }
    }

    // Test: field % modulus == remainder (literals are ignored - modulus and remainder are in
    // constructor)
    boolean result = (fieldValue % modulus) == remainder;
    LOGGER.debug(
        "ModuloEqual.test() field={}, modulus={}, remainder={}, result={} ({}%{}=={})",
        fieldValue, modulus, remainder, result, fieldValue, modulus, fieldValue % modulus);
    return result;
  }

  @Override
  public boolean test(
      DataType type, long rowCount, Object min, Object max, Long nullCount, List<Object> literals) {
    // For statistical pruning, we can't easily determine if any value in range satisfies modulo
    // So we conservatively return true to avoid false negatives
    LOGGER.debug(
        "ModuloEqual.test() (statistical) called with min={}, max={}, rowCount={}, returning true",
        min,
        max,
        rowCount);
    return true;
  }

  @Override
  public Optional<LeafFunction> negate() {
    // No easy negation for modulo operation
    return Optional.empty();
  }

  @Override
  public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
    // This is called during query planning for optimizations like partition pruning
    // Since we have a custom modulo operation that can't be optimized by standard visitors,
    // we need to return a safe default that doesn't interfere with query execution
    LOGGER.debug(
        "ModuloEqual.visit() called with visitor={}, fieldRef={}, modulus={}, remainder={}",
        visitor.getClass().getSimpleName(),
        fieldRef,
        modulus,
        remainder);

    // Check what type of visitor this is and handle accordingly
    String visitorClassName = visitor.getClass().getName();

    if (visitorClassName.contains("FilterPredicate") || visitorClassName.contains("Parquet")) {
      // This is a Parquet filter visitor - return null to indicate no Parquet filtering
      LOGGER.debug("Parquet visitor detected - returning null (no push-down filtering)");
      return null;
    } else {
      // For other visitors (like OnlyPartitionKeyEqualVisitor), return false
      // meaning this predicate cannot be optimized/pushed down
      try {
        @SuppressWarnings("unchecked")
        T result = (T) Boolean.FALSE;
        LOGGER.debug("Boolean visitor detected - returning FALSE (no optimization)");
        return result;
      } catch (ClassCastException e) {
        // Unknown visitor type - log and return null as safest option
        LOGGER.warn(
            "ModuloEqual.visit() called with unknown visitor type: {}. Returning null.",
            visitorClassName);
        return null;
      }
    }
  }
}
