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

package io.delta.standalone.internal.util

import scala.collection.immutable

import com.fasterxml.jackson.databind.JsonNode

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, LessThanOrEqual, Literal}
import io.delta.standalone.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}

/**
 * Results returned by [[DataSkippingUtils.constructDataFilters]]. Contains the column stats
 * predicate, and the set of stats columns appears in the column stats predicate.
 *
 * @param expr The transformed expression for column stats filter.
 * @param referencedStats Columns appears in [[expr]].
 */
private [internal] case class ColumnStatsPredicate(
    expr: Expression,
    referencedStats: immutable.Set[Column])

private[internal] object DataSkippingUtils {

  // TODO: add extensible storage of column stats name and their data type.
  //  (and document if data type is fixed, like type of `NUM_RECORDS` is always LongType)
  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"

  /** Supported data types in column stats */
  final val supportedDataType = Seq(new BinaryType, new BooleanType, new ByteType, new DateType,
    new DoubleType, new FloatType, new IntegerType, new LongType, new ShortType, new StringType,
    new TimestampType)

  /**
   * Parse the stats in AddFile to two maps. The output contains two map distinguishing
   * the file-level stats and column-level stats.
   *
   * For file-level stats, like NUM_RECORDS, it only contains one value per file. The key
   * of file-level stats map is the stats type. And the value of map is stats value.
   *
   * For column-level stats, like MIN, MAX, or NULL_COUNT, they contains one value per column,
   * so that the key of column-level map is the COMPLETE column name with stats type, like `a.MAX`.
   * And the corresponding value of map is the stats value.
   *
   * Since the structured column don't have column stats, no worry about the duplicated column
   * names in column-level stats map. For example: if there is a column has the physical name
   * `a.MAX`, we don't need to worry about it is duplicated with the MAX stats of column `a`,
   * because the structured column `a` doesn't have column stats.
   *
   * If there is a column name not appears in the table schema, we won't store it.
   *
   * Example of `statsString`:
   * {
   *  "numRecords": 3,
   *  "minValues": {
   *      "a": 2,
   *      "b": 1
   *   }, ... // other stats
   * }
   *
   * Currently nested column is not supported.
   *
   * @param tableSchema The table schema describes data column (not stats column) for this query.
   * @param statsString The json-formatted stats in raw string type in AddFile.
   * @return file-level stats map: the map stores file-level stats.
   *         column-level stats map: the map stores column-level stats, like MIN, MAX, NULL_COUNT.
   */
  def parseColumnStats(
      tableSchema: StructType,
      statsString: String): (immutable.Map[String, String], immutable.Map[String, String]) = {
    var fileStats: Map[String, String] = Map()
    var columnStats: Map[String, String] = Map()
    val columnNames = tableSchema.getFieldNames.toSeq
    JsonUtils.fromJson[Map[String, JsonNode]](statsString).foreach { stats =>
      if (!stats._2.isObject) {
        // This is an file-level stats, like ROW_RECORDS.
        if (stats._1 == NUM_RECORDS) {
          fileStats += (stats._1 -> stats._2.asText)
        }
      } else {
        // This is an column-level stats, like MIN_VALUE and MAX_VALUE, iterator through the table
        // schema and fill the column-level stats map column-by-column if the column name appears
        // in json string.
        columnNames.foreach { columnName =>
          // Get stats value by column name
          val statsVal = stats._2.get(columnName)
          if (statsVal != null) {
            val statsType = stats._1
            val statsName = columnName + "." + statsType
            statsType match {
              case MIN | MAX =>
                // Check the stats type for MIN and MAX.
                val dataType = tableSchema.get(columnName).getDataType
                if (DataSkippingUtils.isSupportedType(dataType)) {
                  columnStats += (statsName -> statsVal.asText)
                }
              case NULL_COUNT =>
                columnStats += (statsName -> statsVal.asText)
              case _ =>
            }
          }
        }
      }
    }
    (fileStats, columnStats)
  }

  /**
   * Build the data filter based on query predicate. Now two rules are applied:
   * - (col1 == Literal2) -> (col1.MIN <= Literal2 AND col1.MAX >= Literal2)
   * - constructDataFilters(expr1 AND expr2) ->
   *      constructDataFilters(expr1) AND constructDataFilters(expr2)
   *
   * @param tableSchema The schema describes the structure of stats columns
   * @param expression The non-partition column query predicate.
   * @return columnStatsPredicate: Contains the column stats filter expression, and the set of stat
   *         column that appears in the filter expression, please see [[ColumnStatsPredicate]]
   *         definition.
   */
  def constructDataFilters(
      tableSchema: StructType,
      expression: Expression): Option[ColumnStatsPredicate] =
    expression match {
      case eq: EqualTo => (eq.getLeft, eq.getRight) match {
        case (e1: Column, e2: Literal) =>
          val columnPath = e1.name
          if (!tableSchema.getFieldNames.contains(columnPath)) {
            return None
          }
          val dataType = tableSchema.get(columnPath).getDataType
          if (!DataSkippingUtils.isSupportedType(dataType)) {
            return None
          }
          val minColumn = new Column(columnPath + "." + MIN, dataType)
          val maxColumn = new Column(columnPath + "." + MAX, dataType)

          Some(ColumnStatsPredicate(
            new And(new LessThanOrEqual(minColumn, e2),
              new GreaterThanOrEqual(maxColumn, e2)),
            Set(minColumn, maxColumn)))
        case _ => None
      }
      case and: And =>
        val e1 = constructDataFilters(tableSchema, and.getLeft)
        val e2 = constructDataFilters(tableSchema, and.getRight)

        if (e1.isDefined && e2.isDefined) {
          Some(ColumnStatsPredicate(
            new And(e1.get.expr, e2.get.expr),
            e1.get.referencedStats ++ e2.get.referencedStats))
        } else None

      // TODO: support full types of Expression
      case _ => None
    }

  def isSupportedType(dataType: DataType): Boolean =
    supportedDataType.contains(dataType)
}
