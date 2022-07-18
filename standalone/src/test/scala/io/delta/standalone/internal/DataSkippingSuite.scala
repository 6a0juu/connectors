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

package io.delta.standalone.internal

import java.sql.{Date, Timestamp}

import com.fasterxml.jackson.core.io.JsonEOFException
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, LessThanOrEqual, Literal}
import io.delta.standalone.types.{BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.data.ColumnStatsRowRecord
import io.delta.standalone.internal.util.DataSkippingUtils
import io.delta.standalone.internal.util.TestUtils._

class DataSkippingSuite extends FunSuite {
  private val op = new Operation(Operation.Name.WRITE)

  private val partitionSchema = new StructType(Array(
    new StructField("partitionCol", new LongType(), true)
  ))

  private val schema = new StructType(Array(
    new StructField("partitionCol", new LongType(), true),
    new StructField("col1", new LongType(), true),
    new StructField("col2", new LongType(), true),
    new StructField("stringCol", new StringType(), true)
  ))

  private val nestedSchema = new StructType(Array(
    new StructField("normalCol", new LongType(), true),
    new StructField("parentCol", new StructType(Array(
      new StructField("subCol1", new LongType(), true),
      new StructField("subCol2", new LongType(), true)
    )), true)))

  private val fullTypeSchema = new StructType(Array(
    new StructField("binaryCol", new BinaryType, true),
    new StructField("booleanCol", new BooleanType, true),
    new StructField("byteCol", new ByteType, true),
    new StructField("dateCol", new DateType, true),
    new StructField("doubleCol", new DoubleType, true),
    new StructField("floatCol", new FloatType, true),
    new StructField("integerCol", new IntegerType, true),
    new StructField("longCol", new LongType, true),
    new StructField("shortCol", new ShortType, true),
    new StructField("stringCol", new StringType, true),
    new StructField("timestampCol", new TimestampType, true)))

  private def postfixMax(s: String): String = s"$s.${DataSkippingUtils.MAX}"
  private def postfixMin(s: String): String = s"$s.${DataSkippingUtils.MIN}"

  private val fullTypeColumnStats = Map[String, String](
    postfixMax("binaryCol") -> "ab\"d",
    postfixMax("booleanCol") -> "false",
    postfixMax("byteCol") -> "121",
    postfixMax("dateCol") -> "2022-07-17",
    postfixMax("doubleCol") -> "11.1",
    postfixMax("floatCol") -> "12.2",
    postfixMax("integerCol") -> "123456",
    postfixMax("longCol") -> "4400000000",
    postfixMax("shortCol") -> "32100",
    postfixMax("stringCol") -> "ab\"d",
    postfixMax("timestampCol") -> "2022-05-16 09:00:00"
  )

  val metadata: Metadata = Metadata(partitionColumns = partitionSchema.getFieldNames,
    schemaString = schema.toJson)

  def buildFiles(
      customStats: Option[String] = None): Seq[AddFile] = (1 to 20).map { i =>
    val partitionColValue = i.toString
    val col1Value = (i % 3).toString
    val col2Value = (i % 4).toString
    val stringColValue = "\"a\""
    val partitionValues = Map("partitionCol" -> partitionColValue)
    val fullColumnStats = s"""
      | {
      |   "${DataSkippingUtils.MIN}": {
      |     "partitionCol":$partitionColValue,
      |     "col1":$col1Value,
      |     "col2":$col2Value,
      |     "stringCol":$stringColValue
      |   },
      |   "${DataSkippingUtils.MAX}": {
      |     "partitionCol":$partitionColValue,
      |     "col1":$col1Value,
      |     "col2":$col2Value,
      |     "stringCol":$stringColValue
      |   },
      |   "${DataSkippingUtils.NULL_COUNT}": {
      |     "partitionCol": 0,
      |     "col1": 0,
      |     "col2": 0,
      |     "stringCol": 1
      |   },
      |   "${DataSkippingUtils.NUM_RECORDS}":1
      | }
      |"""

    val columnStats = (if (customStats.isDefined) customStats.get else fullColumnStats)
      .stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString
    // We need to wrap the stats string since it will be parsed twice. Once when AddFile is parsed
    // in LogReplay, and once when stats string it self parsed in DataSkippingUtils.parseColumnStats
    val wrappedColumnStats = "\"" + columnStats.replace("\"", "\\\"") + "\""
    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true, stats = wrappedColumnStats)
  }

  private val nestedFiles = {
    val normalCol = 1
    val subCol1 = 2
    val subCol2 = 3
    val nestedColStats = s"""
      | {
      |   "${DataSkippingUtils.MIN}": {
      |     "normalCol":$normalCol,
      |     "parentCol": {
      |       "subCol1":$subCol1,
      |       "subCol2":$subCol2,
      |     }
      |   },
      |   "${DataSkippingUtils.MAX}": {
      |     "normalCol":$normalCol,
      |     "parentCol": {
      |       "subCol1":$subCol1,
      |       "subCol2":$subCol2,
      |     }
      |   },
      |   "${DataSkippingUtils.NUM_RECORDS}":1
      | }
      |""".stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString
    Seq(AddFile(
      path = "nested",
      Map[String, String](),
      1L,
      1L,
      dataChange = true,
      stats = "\"" + nestedColStats.replace("\"", "\\\"") + "\""))
  }

  private val nestedMetadata: Metadata = Metadata(partitionColumns = Seq[String](),
    schemaString = nestedSchema.toJson)

  private val unwrappedStats = buildFiles().get(0).getStats.replace("\\\"", "\"")
    .dropRight(1).drop(1)

  private val brokenStats = unwrappedStats.substring(0, 10)

  // partition column now supports expression other than equal
  private val metadataConjunct = new LessThanOrEqual(schema.column("partitionCol"), Literal.of(5L))

  private val dataConjunct = new EqualTo(schema.column("col1"), Literal.of(1L))

  def withDeltaLog(
      actions: Seq[Action],
      customMetadata: Option[Metadata]) (l: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val m = customMetadata.getOrElse(metadata)
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(m :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")
      l(log)
    }
  }

  /**
   * The unit test method for constructDataFilter.
   * @param statsString the stats string in JSON format
   * @param fileStatsTarget the target output of file-specific stats
   * @param columnStatsTarget the target output of column-specific stats
   * @param isNestedSchema if we will use nested schema for column stats
   */
  def parseColumnStatsTest(
      statsString: String,
      fileStatsTarget: Map[String, String],
      columnStatsTarget: Map[String, String],
      isNestedSchema: Boolean = false): Unit = {
    val s = if (isNestedSchema) nestedSchema else schema
    val (fileStats, columnStats) = DataSkippingUtils.parseColumnStats(
      tableSchema = s, statsString = statsString)
    assert(fileStats == fileStatsTarget)
    assert(columnStats == columnStatsTarget)
  }

  /**
   * Unit test - parseColumnStats
   */
  test("parse column stats: basic") {
    val fileStatsTarget = Map("numRecords" -> "1")
    val columnStatsTarget = Map(
      "partitionCol.maxValues" -> "1", "col2.nullCount" -> "0", "col2.minValues" -> "1",
      "col1.maxValues" -> "1", "partitionCol.minValues" -> "1", "col2.maxValues" -> "1",
      "stringCol.minValues" -> "a", "stringCol.maxValues" -> "a", "col1.nullCount" -> "0",
      "col1.minValues" -> "1", "stringCol.nullCount" -> "1", "partitionCol.nullCount" -> "0")
    // Though `stringCol` is not LongType, its `nullCount` stats will be documented
    // while `minValues` and `maxValues` won't be.
    parseColumnStatsTest(unwrappedStats, fileStatsTarget, columnStatsTarget)
  }

  test("parse column stats: ignore nested columns") {
    val inputStats = """{"minValues":{"normalCol": 1, "parentCol":{"subCol1": 1, "subCol2": 2}}}"""
    val fileStatsTarget = Map[String, String]()
    val columnStatsTarget = Map("normalCol.minValues" -> "1")
    parseColumnStatsTest(inputStats, fileStatsTarget, columnStatsTarget, isNestedSchema = true)
  }

  test("parse column stats: wrong JSON format") {
    val fileStatsTarget = Map[String, String]()
    val columnStatsTarget = Map[String, String]()
    val e = intercept[JsonEOFException] {
      parseColumnStatsTest(statsString = brokenStats,
        fileStatsTarget, columnStatsTarget)
    }
    assert(e.getMessage.contains("Unexpected end-of-input in field name"))
  }

  test("parse column stats: missing stats from schema") {
    val inputStats = """{"minValues":{"partitionCol": 1, "col1": 2}}"""
    val fileStatsTarget = Map[String, String]()
    val columnStatsTarget = Map[String, String](
      "partitionCol.minValues" -> "1", "col1.minValues" -> "2")
    parseColumnStatsTest(inputStats, fileStatsTarget, columnStatsTarget)
  }

  /**
   * The unit test method for constructDataFilter.
   * @param in              input query predicate
   * @param target          output column stats predicate from
   *                        [[DataSkippingUtils.constructDataFilters]] in string, will be None if
   *                        the method returned empty expression.
   * @param isSchemaMissing if true, testing with empty schema
   */
  def constructDataFilterTest(
      in: Expression,
      target: Option[String],
      isSchemaMissing: Boolean = false): Unit = {
    val tableSchema = if (isSchemaMissing) new StructType(Array()) else schema
    val output = DataSkippingUtils.constructDataFilters(
      tableSchema = tableSchema,
      expression = in)

    assert(output.isDefined == target.isDefined)
    if (target.isDefined) {
      assert(target.get == output.get.expr.toString)
    }
  }

  /**
   * Unit test - constructDataFilters
   */
  test("filter construction: EqualTo") {
    // col1 = 1
    constructDataFilterTest(
      in = new EqualTo(new Column("col1", new LongType), Literal.of(1L)),
      target = Some("((Column(col1.minValues) <= 1) && (Column(col1.maxValues) >= 1))"))
  }

  test("filter construction: AND with EqualTo") {
    // col1 = 1 AND col2 = 1
    constructDataFilterTest(
      in = new And(new EqualTo(new Column("col1", new LongType), Literal.of(1L)),
        new EqualTo(new Column("col2", new LongType), Literal.of(1L))),
      target = Some("(((Column(col1.minValues) <= 1) && (Column(col1.maxValues) >= 1)) &&" +
        " ((Column(col2.minValues) <= 1) && (Column(col2.maxValues) >= 1)))"))
  }

  test("filter construction: the expression '>=' is not supported") {
    // col1 >= 1
    constructDataFilterTest(
      in = new GreaterThanOrEqual(new Column("col1", new LongType), Literal.of(1L)),
      target = None)
  }

  test("filter construction: the expression 'IsNotNull' is not supported") {
    // col1 IS NOT NULL
    constructDataFilterTest(
      in = new IsNotNull(new Column("col1", new LongType)),
      target = None)
  }

  test("filter construction: empty expression will return if schema is missing") {
    // col1 = 1
    constructDataFilterTest(
      in = new EqualTo(new Column("col1", new LongType), Literal.of(1L)),
      target = None,
      isSchemaMissing = true)
  }

  /**
   * The method for integration tests with different query predicate.
   * @param expr              the input query predicate
   * @param target            the file list that is not skipped by evaluating column stats
   * @param customStats       the customized stats string. If none, use default stats
   * @param customFiles       the customized action files. If none, use default files
   * @param customMetadata    the customized metadata. If none, use default metadata
   */
  def filePruningTest(
      expr: Expression,
      target: Seq[String],
      customStats: Option[String] = None,
      customFiles: Option[Seq[AddFile]] = None,
      customMetadata: Option[Metadata] = None): Unit = {
    val logFiles = customFiles.getOrElse(buildFiles(customStats))
    withDeltaLog(logFiles, customMetadata) { log =>
      val scan = log.update().scan(expr)
      val iter = scan.getFiles
      var resFiles: Seq[String] = Seq()
      while (iter.hasNext) {
        // get the index of accepted files
        resFiles = resFiles :+ iter.next().getPath
      }
      assert(resFiles == target)
    }
  }

  /**
   * Integration test
   *
   * Description of the first integration test:
   *
   * - table schema: (partitionCol: long, col1: long, col2: long, stringCol: string)
   *
   * - `files`: rows of data in table, for the i-th file in `files`,
   *      file.path = i, file.partitionCol = i, file.col1 = i % 3, file.col2 = i % 4
   *
   * - range of `i` is from 1 to 20.
   *
   * - the query predicate is `partitionCol <= 5 AND col1 = 1`
   * - [[metadataConjunct]]: the partition predicate expr, which is `partitionCol <= 5`
   * - [[dataConjunct]]: the non-partition predicate expr, which is `col1 = 1`
   *
   * - the accepted files' number should meet the condition: (i <= 5 AND i % 3 == 1)
   *
   * - the output should be: 1, 4.
   */
  test("integration test: column stats filter on 1 partition and 1 non-partition column") {
    filePruningTest(expr = new And(metadataConjunct, dataConjunct),
      target = Seq("1", "4"))
  }

  /**
   * Filter: (i % 3 == 1 AND i % 4 == 1) (1 <= i <= 20)
   * Output: i = 1 or 13
   */
  test("integration test: column stats filter on 2 non-partition column") {
    filePruningTest(expr = new And(new EqualTo(schema.column("col1"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(1L))),
      target = Seq("1", "13"))
  }

  /**
   * Filter: (i % 4 == 1 AND i % 4 == 1) (1 <= i <= 20)
   * Output: i = 1 or 5 or 9 or 13 or 17
   */
  test("integration test: multiple filter on 1 partition column - duplicate") {
    filePruningTest(expr = new And(new EqualTo(schema.column("col2"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(1L))),
      target = Seq("1", "5", "9", "13", "17"))
  }

  /**
   * Filter: (i % 3 == 1 AND i % 3 == 2) (1 <= i <= 20)
   * Output: No file meets the condition
   */
  test("integration test: multiple filter on 1 partition column - conflict") {
    filePruningTest(expr = new And(new EqualTo(schema.column("col1"), Literal.of(1L)),
        new EqualTo(schema.column("col1"), Literal.of(2L))),
      target = Seq())
  }

  /**
   * Filter: (i <= 5 AND i % 3 == 2)
   * Output: i = 1 or 2 or 3 or 4 or 5 (i % 3 == 2 not work)
   * Reason: Because col2.MIN and col2.MAX is used in column stats predicate while not appears in
   * the stats string, we can't evaluate column stats predicate and will skip column stats filter.
   * But the partition column filter still works here.
   */
  test("integration test: missing stats") {
    val incompleteColumnStats =
      s"""
         | {
         |   "${DataSkippingUtils.NULL_COUNT}": {
         |     "partitionCol": 0,
         |     "col1": 0,
         |     "col2": 0,
         |     "stringCol": 1
         |   },
         |   "${DataSkippingUtils.NUM_RECORDS}":1
         | }
         |"""
    filePruningTest(expr = new And(metadataConjunct,
        new EqualTo(schema.column("col2"), Literal.of(2L))),
      target = Seq("1", "2", "3", "4", "5"), Some(incompleteColumnStats))
  }

  /**
   * Filter: (i <= 5 AND i % 4 == 1)
   * Output: i = 1 or 2 or 3 or 4 or 5
   * Reason: Because stats string is empty, we can't evaluate column stats predicate and will skip
   * column stats filter. But the partition column still works here.
   */
  test("integration test: empty stats str") {
    filePruningTest(expr = new And(metadataConjunct,
        new EqualTo(schema.column("col1"), Literal.of(1L))),
      target = Seq("1", "2", "3", "4", "5"), customStats = Some("\"\""))
  }

  /**
   * Filter: (i <= 5 AND i % 4 == 1)
   * Output: i = 1 or 2 or 3 or 4 or 5
   * Reason: Because stats string is broken, we can't evaluate column stats predicate and will skip
   * column stats filter. But the partition column still works here. The JSON parser error is caught
   * in [[io.delta.standalone.internal.scan.FilteredDeltaScanImpl]].
   */
  test("integration test: broken stats str") {
    filePruningTest(expr = new And(metadataConjunct,
        new EqualTo(schema.column("col1"), Literal.of(1L))),
      target = Seq("1", "2", "3", "4", "5"), customStats = Some(brokenStats))
  }

  /**
   * Filter: (i <= 5 AND i == "a")
   * Output: i = 1 or 2 or 3 or 4 or 5
   * Reason: Because decimal type is currently unsupported, we can't evaluate column stats
   * predicate and will skip column stats filter.
   */
  test("integration test: unsupported stats data type") {
    val customSchema = new StructType(Array(
      new StructField("partitionCol", new LongType(), true),
      new StructField("col1", new LongType(), true),
      new StructField("col2", new LongType(), true),
      new StructField("stringCol", new DecimalType(1, 1), true)
    ))
    filePruningTest(expr = new And(metadataConjunct,
        new EqualTo(customSchema.column("stringCol"), Literal.ofNull(new DecimalType(1, 1)))),
      target = Seq("1", "2", "3", "4", "5"),
      customMetadata = Some(Metadata(partitionColumns = partitionSchema.getFieldNames,
        schemaString = customSchema.toJson)))
  }

  /**
   * Filter: (i <= 5 AND i % 3 <= 1)
   * Output: i = 1 or 2 or 3 or 4 or 5
   * Reason: Because LessThanOrEqual is currently unsupported, we can't evaluate column stats
   * predicate and will skip column stats filter.
   */
  test("integration test: unsupported expression type") {

    filePruningTest(expr = new And(metadataConjunct,
        new LessThanOrEqual(schema.column("col1"), Literal.of(1L))),
      target = Seq("1", "2", "3", "4", "5"),
      customMetadata = Some(metadata))
  }

  /**
   * Filter: (parentCol.subCol1 == 1)
   * Output: path = nested
   * Reason: The nested file still returned though it is not qualified in the query predicate.
   * Because nested tables are not supported.
   */
  test("integration test: unsupported nested column") {
    filePruningTest(expr = new EqualTo(nestedSchema.column("normalCol"), Literal.of(1L)),
      target = Seq("nested"),
      customFiles = Some(nestedFiles),
      customMetadata = Some(nestedMetadata))
  }

  /**
   * Test expression evaluation with all supported data types.
   * Type list: Binary, Boolean, Byte, Date, Double, Float, Integer, Short, String, Timestamp
   *
   * @param hits   The expression evaluated as true
   * @param misses The expression evaluated as false
   */
  def dataTypeSupportTest(
      hits: Seq[Expression],
      misses: Seq[Expression]): Unit = {

    val rowRecord = new ColumnStatsRowRecord(fullTypeSchema, Map(), fullTypeColumnStats)
    hits foreach { hit =>
      val result = hit.eval(rowRecord)
      print(hit)
      assert(result != null)
      assert(result.isInstanceOf[Boolean])
      assert(result.asInstanceOf[Boolean])
    }
    misses foreach { miss =>
      val result = miss.eval(rowRecord)
      assert(result != null)
      assert(result.isInstanceOf[Boolean])
      assert(!result.asInstanceOf[Boolean])
    }
  }

  test("all supported data type test") {
    val hits = Seq(
      new EqualTo(new Column(postfixMax("binaryCol"), new BinaryType),
        Literal.of("ab\"d".map(_.toByte).toArray)),
      new EqualTo(new Column(postfixMax("booleanCol"), new BooleanType),
        Literal.of(false)),
      new EqualTo(new Column(postfixMax("byteCol"), new ByteType),
        Literal.of(121.toByte)),
      new EqualTo(new Column(postfixMax("dateCol"), new DateType),
        Literal.of(Date.valueOf("2022-07-17"))),
      new EqualTo(new Column(postfixMax("doubleCol"), new DoubleType),
        Literal.of(11.1D)),
      new EqualTo(new Column(postfixMax("floatCol"), new FloatType),
        Literal.of(12.2F)),
      new EqualTo(new Column(postfixMax("integerCol"), new IntegerType),
        Literal.of(123456)),
      new EqualTo(new Column(postfixMax("longCol"), new LongType),
        Literal.of(4400000000L)),
      new EqualTo(new Column(postfixMax("shortCol"), new ShortType),
        Literal.of(32100.toShort)),
      new EqualTo(new Column(postfixMax("stringCol"), new StringType),
        Literal.of("ab\"d")),
      new EqualTo(new Column(postfixMax("timestampCol"), new TimestampType),
        Literal.of(Timestamp.valueOf("2022-05-16 09:00:00"))),
    )

    val misses = Seq(
      new EqualTo(new Column(postfixMax("binaryCol"), new BinaryType),
        Literal.of("ab\"c".map(_.toByte).toArray)),
      new EqualTo(new Column(postfixMax("booleanCol"), new BooleanType),
        Literal.of(true)),
      new EqualTo(new Column(postfixMax("byteCol"), new ByteType),
        Literal.of(-120.toByte)),
      new EqualTo(new Column(postfixMax("dateCol"), new DateType),
        Literal.of(Date.valueOf("2022-07-19"))),
      new EqualTo(new Column(postfixMax("doubleCol"), new DoubleType),
        Literal.of(11.0D)),
      new EqualTo(new Column(postfixMax("floatCol"), new FloatType),
        Literal.of(12.0F)),
      new EqualTo(new Column(postfixMax("integerCol"), new IntegerType),
        Literal.of(654321)),
      new EqualTo(new Column(postfixMax("longCol"), new LongType),
        Literal.of(3300000000L)),
      new EqualTo(new Column(postfixMax("shortCol"), new ShortType),
        Literal.of(32000.toShort)),
      new EqualTo(new Column(postfixMax("stringCol"), new StringType),
        Literal.of("ab\"de")),
      new EqualTo(new Column(postfixMax("timestampCol"), new TimestampType),
        Literal.of(Timestamp.valueOf("2022-07-16 19:00:00"))),
    )
    dataTypeSupportTest(hits, misses)
  }
}
