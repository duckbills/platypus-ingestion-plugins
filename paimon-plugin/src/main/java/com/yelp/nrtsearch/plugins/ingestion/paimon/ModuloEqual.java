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

/**
 * Custom Paimon predicate function for modulo-based ID sharding. Tests if field % modulus ==
 * remainder (for ID sharding like photo_id % 30 == 23).
 */
public class ModuloEqual extends LeafFunction {

  private final int modulus;
  private final int remainder;

  public ModuloEqual(int modulus, int remainder) {
    this.modulus = modulus;
    this.remainder = remainder;
  }

  @Override
  public boolean test(DataType type, Object field, List<Object> literals) {
    if (field == null) {
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
        return false;
      }
    }

    // Test: field % modulus == remainder (literals are ignored - modulus and remainder are in
    // constructor)
    return (fieldValue % modulus) == remainder;
  }

  @Override
  public boolean test(
      DataType type, long rowCount, Object min, Object max, Long nullCount, List<Object> literals) {
    // For statistical pruning, we can't easily determine if any value in range satisfies modulo
    // So we conservatively return true to avoid false negatives
    return true;
  }

  @Override
  public Optional<LeafFunction> negate() {
    // No easy negation for modulo operation
    return Optional.empty();
  }

  @Override
  public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
    // This is called during query planning - we can provide a generic implementation
    return visitor.visitEqual(fieldRef, remainder); // Approximate as equality for visitor
  }
}
