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

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Apache Paimon InternalRow records to nrtsearch AddDocumentRequest. Handles field mapping
 * and type conversion between Paimon and nrtsearch formats.
 */
public class PaimonToAddDocumentConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PaimonToAddDocumentConverter.class);

  private final PaimonConfig paimonConfig;
  private final Map<String, String> fieldMapping;

  // Will be initialized when we have access to Paimon table schema
  private RowType rowType;

  public PaimonToAddDocumentConverter(PaimonConfig paimonConfig) {
    this.paimonConfig = paimonConfig;
    this.fieldMapping = paimonConfig.getFieldMapping();
  }

  /**
   * Set the Paimon table schema for type-aware conversion. This should be called after table
   * initialization.
   */
  public void setRowType(RowType rowType) {
    this.rowType = rowType;
    LOGGER.info("Initialized converter with Paimon schema: {}", rowType.getFieldNames());
  }

  /** Convert a Paimon InternalRow to nrtsearch AddDocumentRequest. */
  public AddDocumentRequest convertRowToDocument(InternalRow row)
      throws UnrecoverableConversionException {
    if (rowType == null) {
      throw new IllegalStateException("RowType not initialized. Call setRowType() first.");
    }

    AddDocumentRequest.Builder requestBuilder =
        AddDocumentRequest.newBuilder().setIndexName(paimonConfig.getTargetIndexName());
    Map<String, MultiValuedField> fieldMap = new LinkedHashMap<>();

    // Process each field in the row
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      DataField dataField = rowType.getFields().get(i);
      String paimonFieldName = dataField.name();

      // Apply field mapping if configured
      String nrtsearchFieldName =
          fieldMapping != null && fieldMapping.containsKey(paimonFieldName)
              ? fieldMapping.get(paimonFieldName)
              : paimonFieldName;

      try {
        MultiValuedField field = convertField(row, i, nrtsearchFieldName, dataField);
        if (field != null) {
          fieldMap.put(nrtsearchFieldName, field);
        }
      } catch (Exception e) {
        throw new UnrecoverableConversionException(
            "Failed to convert field '"
                + paimonFieldName
                + "' of type "
                + dataField.type().getTypeRoot(),
            e);
      }
    }

    requestBuilder.putAllFields(fieldMap);
    return requestBuilder.build();
  }

  /** Convert a single field from Paimon InternalRow to nrtsearch MultiValuedField. */
  private MultiValuedField convertField(
      InternalRow row, int fieldIndex, String fieldName, DataField dataField) {
    if (row.isNullAt(fieldIndex)) {
      return null; // Skip null fields
    }

    MultiValuedField.Builder fieldBuilder = MultiValuedField.newBuilder();

    // Convert based on Paimon data type
    switch (dataField.type().getTypeRoot()) {
      case BOOLEAN:
        fieldBuilder.addValue(String.valueOf(row.getBoolean(fieldIndex)));
        break;

      case TINYINT:
        fieldBuilder.addValue(String.valueOf(row.getByte(fieldIndex)));
        break;

      case SMALLINT:
        fieldBuilder.addValue(String.valueOf(row.getShort(fieldIndex)));
        break;

      case INTEGER:
        fieldBuilder.addValue(String.valueOf(row.getInt(fieldIndex)));
        break;

      case BIGINT:
        fieldBuilder.addValue(String.valueOf(row.getLong(fieldIndex)));
        break;

      case FLOAT:
        fieldBuilder.addValue(String.valueOf(row.getFloat(fieldIndex)));
        break;

      case DOUBLE:
        fieldBuilder.addValue(String.valueOf(row.getDouble(fieldIndex)));
        break;

      case CHAR:
      case VARCHAR:
        String stringValue = row.getString(fieldIndex).toString();
        fieldBuilder.addValue(stringValue);
        break;

      case DECIMAL:
        // Convert decimal to string representation
        String decimalValue =
            row.getDecimal(
                    fieldIndex,
                    dataField.type().asSQLString().length(),
                    ((org.apache.paimon.types.DecimalType) dataField.type()).getScale())
                .toString();
        fieldBuilder.addValue(decimalValue);
        break;

      case DATE:
        // Convert date to string in ISO format
        int dateValue = row.getInt(fieldIndex); // Days since epoch
        fieldBuilder.addValue(String.valueOf(dateValue));
        break;

      case TIME_WITHOUT_TIME_ZONE:
        // Convert time to string
        int timeValue = row.getInt(fieldIndex); // Milliseconds since midnight
        fieldBuilder.addValue(String.valueOf(timeValue));
        break;

      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // Convert timestamp to long (milliseconds since epoch)
        long timestampValue =
            row.getTimestamp(
                    fieldIndex,
                    ((org.apache.paimon.types.TimestampType) dataField.type()).getPrecision())
                .getMillisecond();
        fieldBuilder.addValue(String.valueOf(timestampValue));
        break;

      case BINARY:
      case VARBINARY:
        // Convert binary data to base64 string
        byte[] binaryValue = row.getBinary(fieldIndex);
        String base64Value = java.util.Base64.getEncoder().encodeToString(binaryValue);
        fieldBuilder.addValue(base64Value);
        break;

      case ARRAY:
        // Convert array to JSON string representation
        org.apache.paimon.data.InternalArray arrayValue = row.getArray(fieldIndex);
        fieldBuilder.addValue(convertArrayToJson(arrayValue, dataField.type()));
        break;

      case MAP:
        // Convert map to JSON string representation
        org.apache.paimon.data.InternalMap mapValue = row.getMap(fieldIndex);
        fieldBuilder.addValue(convertMapToJson(mapValue, dataField.type()));
        break;

      case ROW:
        // Convert nested row to JSON string representation
        InternalRow nestedRow = row.getRow(fieldIndex, rowType.getFieldCount());
        fieldBuilder.addValue(convertRowToJson(nestedRow, dataField.type()));
        break;

      default:
        LOGGER.warn(
            "Unsupported Paimon field type: {} for field: {}",
            dataField.type().getTypeRoot(),
            fieldName);
        return null;
    }

    return fieldBuilder.build();
  }

  /** Convert Paimon array to JSON string representation. */
  private String convertArrayToJson(
      org.apache.paimon.data.InternalArray array, org.apache.paimon.types.DataType dataType) {
    // Simplified JSON conversion - in production, use proper JSON library
    StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < array.size(); i++) {
      if (i > 0) json.append(",");
      // Add array element conversion based on element type
      json.append("\"").append(array.getString(i)).append("\"");
    }
    json.append("]");
    return json.toString();
  }

  /** Convert Paimon map to JSON string representation. */
  private String convertMapToJson(
      org.apache.paimon.data.InternalMap map, org.apache.paimon.types.DataType dataType) {
    // Simplified JSON conversion - in production, use proper JSON library
    StringBuilder json = new StringBuilder("{");
    org.apache.paimon.data.InternalArray keys = map.keyArray();
    org.apache.paimon.data.InternalArray values = map.valueArray();

    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) json.append(",");
      json.append("\"").append(keys.getString(i)).append("\":");
      json.append("\"").append(values.getString(i)).append("\"");
    }
    json.append("}");
    return json.toString();
  }

  /** Convert nested Paimon row to JSON string representation. */
  private String convertRowToJson(
      InternalRow nestedRow, org.apache.paimon.types.DataType dataType) {
    // Simplified JSON conversion - in production, use proper JSON library
    return "{\"nested\":\"row\"}"; // Placeholder implementation
  }

  /**
   * Exception indicating a record cannot be converted due to permanent data issues. These are
   * "poison pill" records that should be skipped, not retried.
   */
  public static class UnrecoverableConversionException extends Exception {
    public UnrecoverableConversionException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
