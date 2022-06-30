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

import io.delta.standalone.data.RowRecord
import io.delta.standalone.types.StructType

/**
 * The record for data mapping
 * @param columnSchema abc
 * @param columnValue def
 */
class ColumnStatsRowRecord(
    columnSchema: StructType,
    columnValue: Map[String, String]) extends RowRecord {

  override def getSchema: StructType = columnSchema

  override def getLength: Int = columnValue.size

  override def isNullAt(fieldName: String): Boolean = !columnValue.contains(fieldName)

  override def getInt(fieldName: String): Int =
    columnValue.getOrElse(fieldName, null).toInt

  override def getLong(fieldName: String): Long =
    columnValue.getOrElse(fieldName, null).toLong

  override def getByte(fieldName: String): Byte =
    columnValue.getOrElse(fieldName, null).toByte

  override def getShort(fieldName: String): Short =
    columnValue.getOrElse(fieldName, null).toShort

  override def getBoolean(fieldName: String): Boolean =
    columnValue.getOrElse(fieldName, null).toBoolean

  override def getFloat(fieldName: String): Float =
    columnValue.getOrElse(fieldName, null).toFloat

  override def getDouble(fieldName: String): Double =
    columnValue.getOrElse(fieldName, null).toDouble

  override def getString(fieldName: String): String =
    columnValue.getOrElse(fieldName, null)

  override def getBinary(fieldName: String): Array[Byte] =
    columnValue.getOrElse(fieldName, null).map(_.toByte).toArray

  override def getBigDecimal(fieldName: String): java.math.BigDecimal =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getTimestamp(fieldName: String): Timestamp =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getDate(fieldName: String): Date =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getRecord(fieldName: String): RowRecord =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getList[T](fieldName: String): util.List[T] =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getMap[K, V](fieldName: String): util.Map[K, V] =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")
}
