/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal.data

import java.sql.{Date, Timestamp}
import java.util

import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.types.StructType

import io.delta.standalone.internal.util.{DataSkippingUtils, SchemaUtils}

/**
 * Provide table schema and stats value for columns evaluation, with various of data types.
 *
 * Example: Assume we have a table schema like this: table1(col1: long, col2:long)),
 * with the only supported stats type NUM_RECORDS and MIN.
 *
 * [[fileValues]] will be like: {"NUM_RECORDS": 1}
 * [[columnValues]] will be like:
 * {
 *  "col1.MIN": 2,
 *  "col2.MIN": 3
 * }
 *
 * When evaluating the expression EqualTo(Column("NUM_RECORDS", Long), Column("col2.MIN", Long)),
 * [[getStringOrNone]] would be called twice to get the data from two maps.
 *
 * In [[getStringOrNone]], it will firstly distinguish the stats type, like MIN is a column-specific
 * class or NUM_RECORDS is a file-specific class, then find the stats value in the corresponding
 * map, then return the value for Column evaluation.
 *
 * When calling [[getStringOrNone]] for Column("NUM_RECORDS", Long), since NUM_RECORDS is a
 * file-specific stats, so returning fileValues["NUM_RECORDS"].
 *
 * When calling [[getStringOrNone]] for Column("col2.MIN", Long), since MIN is a column-specific
 * class, so returning columnValues["col2.MIN"].
 *
 * The [[getStringOrNone]] function guarantees to return a string value during evaluation,
 * because [[isNullAt]] function is called before [[getStringOrNone]] to pre-check that there is
 * a value with correct data type and correct column name stored in the map.
 *
 * It is also guaranteed that the [[IsNull]] or [[IsNotNull]] won't appears in column stats
 * predicate, so [[isNullAt]] will only work for pre-checking when evaluating the column.
 *
 * @param tableSchema The schema of scanning table, loaded from table metadata.
 * @param fileValues file-specific stats, like NUM_RECORDS.
 * @param columnValues column-specific stats, like MIN, MAX, or NULL_COUNT.
 */
private[internal] class ColumnStatsRowRecord(
    tableSchema: StructType,
    fileValues: collection.Map[String, String],
    columnValues: collection.Map[String, String]) extends RowRecordJ {

  override def getSchema: StructType = tableSchema

  override def getLength: Int = tableSchema.length()

  /** Return a None if the stats is not found, return Some(String) if it's found. */
  def getStringOrNone(fieldName: String): Option[String] = {
    // Parse column name with stats type: a.MAX => Seq(a, MAX)
    val pathToColumn = SchemaUtils.parseAndValidateColumn(fieldName)

    // In stats column the last element is stats type
    val statsType = pathToColumn.last

    statsType match {
      // For the file-level column, like NUM_RECORDS, we get value from fileValues map by
      // stats type.
      case DataSkippingUtils.NUM_RECORDS if pathToColumn.length == 1 =>
        // File-level column name should only have the stats type as name
        fileValues.get(fieldName)

      // For the column-level stats type, like MIN or MAX, we get value from columnValues map
      // by the COMPLETE column name with stats type, like `a.MAX`.
      case DataSkippingUtils.NULL_COUNT if pathToColumn.length == 2 =>
        // Currently we only support non-nested columns, so the `pathToColumn` should only contain
        // 2 parts: the column name and the stats type.
        columnValues.get(fieldName)

      case DataSkippingUtils.MIN | DataSkippingUtils.MAX if pathToColumn.length == 2 =>
        val columnName = pathToColumn.head
        if (tableSchema.getFieldNames.contains(columnName)) {
          // ensure that such column name exists in table schema
          val dataTypeInSchema = tableSchema.get(columnName).getDataType
          if (DataSkippingUtils.isSupportedType(dataTypeInSchema)) {
            // ensure that the data type in table schema is the same as it in stats storage.
            columnValues.get(fieldName)
          } else None
        } else None

      case _ => None
    }
  }

  /** if a column not exists either in value map, then it is missing, will return true. */
  override def isNullAt(fieldName: String): Boolean = {
    getStringOrNone(fieldName).isEmpty
  }

  override def getInt(fieldName: String): Int = getStringOrNone(fieldName).get.toInt

  /** getLongOrNone must return the field name here as we have pre-checked by [[isNullAt]] */
  override def getLong(fieldName: String): Long = getStringOrNone(fieldName).get.toLong

  override def getByte(fieldName: String): Byte = getStringOrNone(fieldName).get.toByte

  override def getShort(fieldName: String): Short = getStringOrNone(fieldName).get.toShort

  override def getBoolean(fieldName: String): Boolean = getStringOrNone(fieldName).get.toBoolean

  override def getFloat(fieldName: String): Float = getStringOrNone(fieldName).get.toFloat

  override def getDouble(fieldName: String): Double = getStringOrNone(fieldName).get.toDouble

  override def getString(fieldName: String): String = getStringOrNone(fieldName).get

  override def getBinary(fieldName: String): Array[Byte] =
    getStringOrNone(fieldName).map(_.map(_.toByte).toArray).orNull

  override def getBigDecimal(fieldName: String): java.math.BigDecimal =
    throw new UnsupportedOperationException("BigDecimal is not a supported column stats type.")

  override def getTimestamp(fieldName: String): Timestamp = {
    val stringValue = getStringOrNone(fieldName)
    try {
      stringValue.map(Timestamp.valueOf).orNull
    } catch {
      case _: Exception => null
    }
  }

  override def getDate(fieldName: String): Date = {
    val stringValue = getStringOrNone(fieldName)
    try {
      stringValue.map(Date.valueOf).orNull
    } catch {
      case _: Exception => null
    }
  }

  override def getRecord(fieldName: String): RowRecordJ =
    throw new UnsupportedOperationException("Record is not a supported column stats type.")

  override def getList[T](fieldName: String): util.List[T] =
    throw new UnsupportedOperationException("List is not a supported column stats type.")

  override def getMap[K, V](fieldName: String): util.Map[K, V] =
    throw new UnsupportedOperationException("Map is not a supported column stats type.")
}
