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

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.util

import io.delta.standalone.data.RowRecord
import io.delta.standalone.types.StructType

import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.scan.ColumnNode

/**
 * The record for data mapping
 * @param columnSchema the table schema to get data type
 * @param statsValue the corresponding column node in stats store
 */
private[internal] class ColumnStatsRowRecord(
    columnSchema: StructType,
    statsValue: ColumnNode) extends RowRecord {

  private def getRawString(fieldName: Array[String]): Option[String] = {
    var curStats = statsValue
    val statsType = fieldName.last
    fieldName.dropRight(1).foreach { x =>
      curStats = curStats.nestedColumns.getOrElse(x,
        throw DeltaErrors.nullValueFoundForNonNullSchemaField(
          fieldName.mkString("Column(", ".", ")"), columnSchema))
    }
    curStats.statsValue.get(statsType)
  }

  override def getSchema: StructType = columnSchema

  override def getLength: Int = statsValue.nestedColumns.size

  override def isNullAt(fieldName: Array[String]): Boolean =
    getRawString(fieldName).isEmpty

  override def getInt(fieldName: Array[String]): Int =
    getRawString(fieldName).orNull.toInt

  override def getLong(fieldName: Array[String]): Long =
    getRawString(fieldName).orNull.toLong

  override def getByte(fieldName: Array[String]): Byte =
    getRawString(fieldName).orNull.toByte

  override def getShort(fieldName: Array[String]): Short =
    getRawString(fieldName).orNull.toShort

  override def getBoolean(fieldName: Array[String]): Boolean =
    getRawString(fieldName).orNull.toBoolean

  override def getFloat(fieldName: Array[String]): Float =
    getRawString(fieldName).orNull.toFloat

  override def getDouble(fieldName: Array[String]): Double =
    getRawString(fieldName).orNull.toDouble

  override def getString(fieldName: Array[String]): String = {
    getRawString(fieldName).orNull
    // TODO long string truncation
  }

  override def getBinary(fieldName: Array[String]): Array[Byte] =
    getRawString(fieldName).orNull.map(_.toByte).toArray

  override def getBigDecimal(fieldName: Array[String]): BigDecimal =
    getRawString(fieldName).map(new BigDecimal(_)).orNull

  override def getTimestamp(fieldName: Array[String]): Timestamp = {
    getRawString(fieldName).map(Timestamp.valueOf).orNull
    // TODO timestamp truncation
  }

  override def getDate(fieldName: Array[String]): Date =
    getRawString(fieldName).map(Date.valueOf).orNull

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
