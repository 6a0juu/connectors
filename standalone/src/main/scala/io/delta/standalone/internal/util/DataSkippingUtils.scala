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

import com.fasterxml.jackson.databind.JsonNode

import io.delta.standalone.expressions.{And, BinaryComparison, Column, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Or}
import io.delta.standalone.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructField, StructType}

import io.delta.standalone.internal.exception.DeltaErrors

private[internal] object DataSkippingUtils {

  // TODO: add extensible storage of column stats name and their data type.
  //  (data type can be fixed, like type of `NUM_RECORDS` is always LongType)
  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"

  /* The file-specific stats column contains only the column type. e.g.: NUM_RECORD */
  final val fileStatsPathLength = 1
  /* The column-specific stats column contains column type and column name. e.g.: MIN.col1 */
  final val columnStatsPathLength = 2

  /* Supported data types in column stats filter */
  final val supportedDataType = Seq(new BooleanType, new ByteType, new DoubleType,
    new FloatType, new IntegerType, new LongType, new ShortType)

  /**
   * Build stats schema based on the schema of data columns, the first layer
   * of stats schema is stats type. If it is a column-specific stats, it nested a second layer,
   * which contains the column name in table schema. Or if it is a column-specific stats, contains
   * a non-nested data type.
   *
   * The layout of stats schema is totally the same as the full stats string in JSON:
   * (for the table contains column `a` and `b`)
   * {
   *  "[[NUM_RECORDS]]": 3,
   *  "[[MIN]]": {
   *      "a": 2,
   *      "b": 1
   *   }, ... // other stats
   * }
   *
   * @param dataSchema The schema of data columns in table.
   * @return The schema storing the layout of stats columns.
   */
  def buildStatsSchema(dataSchema: StructType): StructType = {
    // TODO: add partial stats support as config `DATA_SKIPPING_NUM_INDEXED_COLS`
    val nonNestedColumns = dataSchema
      .getFields
      .filterNot(_.getDataType.isInstanceOf[StructType])
    nonNestedColumns.length match {
      case 0 => new StructType()
      case _ =>
        val nullCountColumns = nonNestedColumns.map { field =>
          new StructField(field.getName, new LongType)
        }
        new StructType(Array(
          // MIN and MAX are used the corresponding data column's type.
          new StructField(MIN, new StructType(nonNestedColumns)),
          new StructField(MAX, new StructType(nonNestedColumns)),

          // nullCount is using the LongType for all columns
          new StructField(NULL_COUNT, new StructType(nullCountColumns)),

          // numRecords is a file-specific Long value
          new StructField(NUM_RECORDS, new LongType)))
    }
  }

  /**
   * Parse the stats in data metadata files to two maps. The output contains two maps
   * distinguishing the file-specific stats and column-specific stats.
   *
   * For file-specific stats, like NUM_RECORDS, it only contains one value per file. The key
   * of file-specific stats map is the stats type. And the value of map is stats value.
   *
   * For column-specific stats, like MIN, MAX, or NULL_COUNT, they contains one value per column at
   * most, so the key of column-specific map is the stats type with COMPLETE column name, like
   * `MAX.a`. And the corresponding value of map is the stats value.
   *
   * If there is a column name not appears in the table schema, we won't store it.
   *
   * Example of `statsString` (for the table contains column `a` and `b`):
   * {
   *   "[[NUM_RECORDS]]": 3,
   *   "[[MIN]]": {
   *      "a": 2,
   *      "b": 1
   *   }, ... // other stats
   * }
   *
   * The corresponding output will be:
   * fileStats = Map("[[NUM_RECORDS]]" -> 3)
   * columnStats = Map("[[MIN]].a" -> 2, "[[MIN]].b" -> 1)
   *
   * If encountered a wrong data type with a known stats type, the method will raise error and
   * should be handled by caller.
   *
   * @param dataSchema  The schema of data columns in table.
   * @param statsString The JSON-formatted stats in raw string type in table metadata files.
   * @return file-specific stats map:   The map stores file-specific stats, like [[NUM_RECORDS]].
   *         column-specific stats map: The map stores column-specific stats, like [[MIN]],
   *         [[MAX]], [[NULL_COUNT]].
   */
  def parseColumnStats(
      dataSchema: StructType,
      statsString: String): (Map[String, String], Map[String, String]) = {
    var fileStats = Map[String, String]()
    var columnStats = Map[String, String]()

    val dataColumns = dataSchema.getFields
    JsonUtils.fromJson[Map[String, JsonNode]](statsString).foreach { case (statsType, statsObj) =>
      if (!statsObj.isObject) {
        // This is an file-specific stats, like ROW_RECORDS.
        val statsVal = statsObj.asText
        if (statsType == NUM_RECORDS) {
          checkValueFormat(statsType, statsVal, new LongType)
          fileStats += (statsType -> statsVal)
        }
      } else {
        // This is an column-specific stats, like MIN_VALUE and MAX_VALUE, iterator through the
        // schema of data columns and fill the column-specific stats map column-by-column if the
        // column name appears in JSON string.
        dataColumns.filter(col => statsObj.has(col.getName)).foreach { dataColumn =>
          // Get stats value by column in data schema.
          val columnName = dataColumn.getName
          val statsVal = statsObj.get(columnName)
          if (statsVal != null) {
            val statsValStr = statsVal.asText
            val statsName = statsType + "." + dataColumn.getName
            statsType match {
              case MIN | MAX =>
                // Check the stats type for MIN and MAX.
                if (isValidType(dataColumn.getDataType)) {
                  checkValueFormat(statsType, statsValStr, dataColumn.getDataType)
                  columnStats += (statsName -> statsValStr)
                }
              case NULL_COUNT =>
                checkValueFormat(statsType, statsValStr, new LongType)
                columnStats += (statsName -> statsValStr)
              case _ =>
            }
          }
        }
      }
    }
    (fileStats, columnStats)
  }

  /** Building [[Column]] referencing stats value. */
  def statsColumnBuilder(statsType: String, columnName: String, dataType: DataType): Column =
    new Column(statsType + "." + columnName, dataType)

  /**
   * Building min/max stats column with given column name. Return None if the column is not found
   * in schema or its data type is wrong.
   */
  def getMinMaxColumn(dataSchema: StructType, columnPath: String): Option[(Column, Column)] = {
    if (!dataSchema.contains(columnPath)) {
      return None
    }
    val dataType = dataSchema.get(columnPath).getDataType
    if (!isValidType(dataType)) {
      return None
    }
    Some(
      statsColumnBuilder(MIN, columnPath, dataType),
      statsColumnBuilder(MAX, columnPath, dataType))
  }

  /**
   * Build column stats predicate based on [[BinaryComparison]] expression in query predicate.
   * Specifically, this method will apply one of the rules in parameter based on the expression
   * type of left and right children.
   *
   * Supported expressions are: LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, EqualTo
   *
   * We only handle the case when `Column` or `Literal` value is the child of binary comparator.
   * Because they are the only two leaf expression types.
   *
   * @param dataSchema The schema of data columns in table.
   * @param expr       The expression from query predicate.
   * @param ccRule     The building rule when left and right children are both columns.
   * @param clRule     The building rule when left child is a column and right child is a literal
   *                   value.
   * @param lcRule     The building rule when left child is a literal value and left child is a
   *                   column.
   * @return columnStatsPredicate: Return the column stats filter predicate. Or it will return None
   *         if met unsupported data type, or unsupported expression type issues.
   */
  def buildBinaryComparatorFilter(
      dataSchema: StructType,
      expr: BinaryComparison,
      ccRule: (Column, Column, Column, Column) => Expression,
      clRule: (Column, Column, Literal) => Expression,
      lcRule: (Literal, Column) => Expression): Option[Expression] = {
    (expr.getLeft, expr.getRight) match {
      case (e1: Column, e2: Column) =>
        val leftMinMaxCol = getMinMaxColumn(dataSchema, e1.name).getOrElse { return None }
        val rightMinMaxCol = getMinMaxColumn(dataSchema, e2.name).getOrElse { return None }
        Some(ccRule(leftMinMaxCol._1, leftMinMaxCol._2, rightMinMaxCol._1, rightMinMaxCol._2))
      case (e1: Column, e2: Literal) =>
        val leftMinMaxCol = getMinMaxColumn(dataSchema, e1.name).getOrElse { return None }
        Some(clRule(leftMinMaxCol._1, leftMinMaxCol._2, e2))
      case (e1: Literal, e2: Column) =>
        constructDataFilters(dataSchema, Some(lcRule(e1, e2)))

      // If left and right children are both literal value, we return the original expression.
      case (_: Literal, _: Literal) => Some(expr)
      case _ => None
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Build the column stats filter based on query predicate and the schema of data columns.
   * Rules are derived from:
   * https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/stats/DataSkippingReader.scala
   *
   * @param dataSchema      The schema of data columns in table.
   * @param dataConjunction The non-partition column query predicate.
   * @return columnStatsPredicate: Return the column stats filter predicate. Or it will return None
   *         if met unsupported data type, or unsupported expression type issues.
   */
  // scalastyle:on line.size.limit
  def constructDataFilters(
      dataSchema: StructType,
      dataConjunction: Option[Expression]): Option[Expression] = {
    // For the following comments, assume `col1`, `col2` are columns in query predicate, `l1` is a
    // literal values in query predicate, and `expr1`, `expr2` are two predicates in the query
    // predicate. Let `f` be this method `constructDataFilters`.
    val expr = dataConjunction.getOrElse{ return None }
    expr match {
      case eq: EqualTo =>
        // (col1 == l1) -> (MIN.col1 <= l1 AND MAX.col1 >= l1)
        val clRule = (minCol: Column, maxCol: Column, e2: Literal) =>
          new And(
            new LessThanOrEqual(minCol, e2),
            new GreaterThanOrEqual(maxCol, e2))

        // (l1 == col1) -> (col1 == l1)
        val lcRule = (e1: Literal, e2: Column) => new EqualTo(e2, e1)

        // (col1 == col2) -> (MIN.col1 <= MAX.col2 AND MAX.col1 >= MIN.col2)
        val ccRule = (e1Min: Column, e1Max: Column, e2Min: Column, e2Max: Column) =>
          new And(
            new LessThanOrEqual(e1Min, e2Max),
            new GreaterThanOrEqual(e1Max, e2Min))
        buildBinaryComparatorFilter(dataSchema, eq, ccRule, clRule, lcRule)

      case lt: LessThan =>
        // (col1 < l1) -> (MIN.col1 < l1)
        val clRule = (minCol: Column, _: Column, e2: Literal) =>
          new LessThan(minCol, e2)

        // (l1 < col1) -> (col1 > l1)
        val lcRule = (e1: Literal, e2: Column) => new GreaterThan(e2, e1)

        // (col1 < col2) -> (MIN.col1 < MAX.col2)
        val ccRule = (e1Min: Column, _: Column, _: Column, e2Max: Column) =>
          new LessThan(e1Min, e2Max)
        buildBinaryComparatorFilter(dataSchema, lt, ccRule, clRule, lcRule)

      case gt: GreaterThan =>
        // (col1 > l1) -> (MAX.col1 > l1)
        val clRule = (_: Column, maxCol: Column, e2: Literal) =>
          new GreaterThan(maxCol, e2)

        // (l1 > col1) -> (col1 < l1)
        val lcRule = (e1: Literal, e2: Column) => new LessThan(e2, e1)

        // (col1 > col2) -> (MAX.col1 > MIN.col2)
        val ccRule = (_: Column, e1Max: Column, e2Min: Column, _: Column) =>
          new GreaterThan(e1Max, e2Min)
        buildBinaryComparatorFilter(dataSchema, gt, ccRule, clRule, lcRule)

      case leq: LessThanOrEqual =>
        // (col1 <= l1) -> (MIN.col1 <= l1)
        val clRule = (minCol: Column, _: Column, e2: Literal) =>
          new LessThanOrEqual(minCol, e2)

        // (l1 <= col1) -> (col1 >= l1)
        val lcRule = (e1: Literal, e2: Column) => new GreaterThanOrEqual(e2, e1)

        // (col1 <= col2) -> (MIN.col1 <= MAX.col2)
        val ccRule = (e1Min: Column, _: Column, _: Column, e2Max: Column) =>
          new LessThanOrEqual(e1Min, e2Max)
        buildBinaryComparatorFilter(dataSchema, leq, ccRule, clRule, lcRule)

      case geq: GreaterThanOrEqual =>
        // (col1 >= l1) -> (MAX.col1 >= l1)
        val clRule = (_: Column, maxCol: Column, e2: Literal) =>
          new GreaterThanOrEqual(maxCol, e2)

        // (l1 >= col1) -> (col1 <= l1)
        val lcRule = (e1: Literal, e2: Column) => new LessThanOrEqual(e2, e1)

        // (col1 >= col2) -> (MAX.col1 >= MIN.col2)
        val ccRule = (_: Column, e1Max: Column, e2Min: Column, _: Column) =>
          new GreaterThanOrEqual(e1Max, e2Min)
        buildBinaryComparatorFilter(dataSchema, geq, ccRule, clRule, lcRule)

      case and: And =>
        val e1 = constructDataFilters(dataSchema, Some(and.getLeft))
        val e2 = constructDataFilters(dataSchema, Some(and.getRight))
        (e1, e2) match {
          // f(expr1 AND expr2) -> f(expr1) AND f(expr2)
          case (Some(e1), Some(e2)) => Some(new And(e1, e2))

          // f(expr1 AND null) -> f(expr1)
          case (Some(e1), _) => Some(e1)

          // f(null AND expr2) -> f(expr2)
          case (_, Some(e2)) => Some(e2)
          case _ => None
        }

      case or: Or =>
        val e1 = constructDataFilters(dataSchema, Some(or.getLeft))
        val e2 = constructDataFilters(dataSchema, Some(or.getRight))
        (e1, e2) match {
          // f(expr1 OR expr2) -> f(expr1) OR f(expr2)
          case (Some(e1), Some(e2)) => Some(new Or(e1, e2))
          case _ => None
        }

      // TODO: support full types of Expression
      case _ => None
    }
  }

  /**
   * Return true if the give data type is supported in column stats filter, otherwise return false.
   */
  def isValidType(dataType: DataType): Boolean = supportedDataType.contains(dataType)

  /**
   * Checking stats value format with the given data type. Will raise wrong data format exception if
   * stats value is in wrong format. The exception should be handled by the caller.
   */
  def checkValueFormat(fieldName: String, v: String, dataType: DataType): Unit = dataType match {
    case _: BooleanType => v.toBoolean
    case _: ByteType => v.toByte
    case _: DoubleType => v.toDouble
    case _: FloatType => v.toFloat
    case _: IntegerType => v.toInt
    case _: LongType => v.toLong
    case _: ShortType => v.toShort
    case _ => throw DeltaErrors.fieldTypeMismatch(fieldName, dataType, "Unknown Type")
  }
}
