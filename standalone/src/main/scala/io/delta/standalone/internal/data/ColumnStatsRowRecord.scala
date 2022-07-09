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
private[internal] class ColumnStatsRowRecord(
    columnSchema: StructType,
    columnValue: StructType) extends RowRecord {

  override def getSchema: StructType = columnSchema

  override def getLength: Int = columnValue.length()

  override def isNullAt(fieldName: Array[String]): Boolean = false

  override def getInt(fieldName: Array[String]): Int =
    fieldName.head.toInt

  override def getLong(fieldName: Array[String]): Long =
    columnValue.get(fieldName.head).getMetadata.get("123").toString.toInt

  override def getByte(fieldName: Array[String]): Byte =
    fieldName.head.toByte

  override def getShort(fieldName: Array[String]): Short =
    fieldName.head.toShort

  override def getBoolean(fieldName: Array[String]): Boolean =
    fieldName.head.toBoolean

  override def getFloat(fieldName: Array[String]): Float =
    fieldName.head.toFloat

  override def getDouble(fieldName: Array[String]): Double =
    fieldName.head.toDouble

  override def getString(fieldName: Array[String]): String =
    fieldName.head

  override def getBinary(fieldName: Array[String]): Array[Byte] =
    fieldName.head.map(_.toByte).toArray

  override def getBigDecimal(fieldName: Array[String]): java.math.BigDecimal =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getTimestamp(fieldName: Array[String]): Timestamp =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getDate(fieldName: Array[String]): Date =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getRecord(fieldName: Array[String]): RowRecord =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getList[E](fieldName: Array[String]): util.List[E] =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")

  override def getMap[K, V](fieldName: Array[String]): util.Map[K, V] =
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")
}
