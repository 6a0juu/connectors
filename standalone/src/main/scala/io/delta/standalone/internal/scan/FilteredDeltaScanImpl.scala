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

package io.delta.standalone.internal.scan

import java.util.Optional

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, IsNull, LessThanOrEqual, Literal, Not, Or}
import io.delta.standalone.types.{BooleanType, DataType, DateType, LongType, StringType, StructField, StructType, TimestampType}

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay, Metadata}
import io.delta.standalone.internal.data.{ColumnStatsRowRecord, PartitionRowRecord, RowParquetRecordImpl}
import io.delta.standalone.internal.util.{DataTypeParser, JsonUtils, PartitionUtils}


private [internal] case class StatsColumn(
    statType: String,
    pathToColumn: Seq[String] = Nil)

private [internal] case class ColumnStatsPredicate(
    expr: Expression,
    referencedStats: Set[StatsColumn])

private [internal] case class AddFileStats(
    numRecords: Long,
    nullCount: Map[String, Long],
    minValues: Map[String, String],
    maxValues: Map[String, String])

/**
 * An implementation of [[io.delta.standalone.DeltaScan]] that filters files and only returns
 * those that match the [[getPushedPredicate]].
 *
 * If the pushed predicate is empty, then all files are returned.
 */
final private[internal] class FilteredDeltaScanImpl(
    replay: MemoryOptimizedLogReplay,
    expr: Expression,
    partitionSchema: StructType,
    metadata: Metadata) extends DeltaScanImpl(replay) {
  // TODO: Metadata -> Metadata.dataSchema = NULL

  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"

  private val partitionColumns = partitionSchema.getFieldNames.toSeq

  private val (metadataConjunction, dataConjunction) =
    PartitionUtils.splitMetadataAndDataPredicates(expr, partitionColumns)

  override protected def accept(addFile: AddFile): Boolean = {
    if (metadataConjunction.isEmpty && dataConjunction.isEmpty) return true

    val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
    val result = metadataConjunction.get.eval[String](partitionRowRecord).asInstanceOf[Boolean]

    // parse min max in column stats
    val statsValue = JsonUtils.fromJson[AddFileStats](addFile.stats)
    // keep min max stats only
    val minMaxValues: Map[String, String] =
      (statsValue.maxValues map {case (k, v) => (k + "." + MAX, v)}).++(
        statsValue.minValues map {case (k, v) => (k + "." + MIN, v)})
    // TODO construct recursive all column stats by StructType &&
    // TODO find values in the StructType in ColumnStatsRowRecord
    val minMaxStruct: StructType = buildColumnStats(metadata.dataSchema, addFile.stats)

    // make conjunctions by min/max and dataConjunction
    val columnStatsPredicate = constructDataFilters(dataConjunction.get).getOrElse {
      return result
    }

    // eval, get Boolean result, and return
    val dataRowRecord = new ColumnStatsRowRecord(metadata.dataSchema, minMaxStruct)

    val finalColumnStatsPredicate = columnStatsPredicate
    // Add verifyStatsForFilter later
    val columnStatsFilterResult = finalColumnStatsPredicate
      .eval(dataRowRecord)
      .asInstanceOf[Boolean]

    result && columnStatsFilterResult
  }

  override def getInputPredicate: Optional[Expression] = Optional.of(expr)

  override def getPushedPredicate: Optional[Expression] =
    Optional.ofNullable(metadataConjunction.orNull)

  override def getResidualPredicate: Optional[Expression] =
    Optional.ofNullable(dataConjunction.orNull)

  private def constructDataFilters(expression: Expression):
    Option[Expression] = expression match {
    case andExpr: And =>
      val e1 = andExpr.getLeft
      val e2 = andExpr.getRight
      val e1Filter = constructDataFilters(e1)
      val e2Filter = constructDataFilters(e2)
      if (e1Filter.isDefined && e2Filter.isDefined) {
        Some(new And(e1Filter.get, e2Filter.get))
      } else if (e1Filter.isDefined) {
        e1Filter
      } else {
        e2Filter  // possibly None
      }

    case orExpr: Or =>
      val e1 = orExpr.getLeft
      val e2 = orExpr.getRight
      val e1Filter = constructDataFilters(e1)
      val e2Filter = constructDataFilters(e2)
      if (e1Filter.isDefined && e2Filter.isDefined) {
        Some(new Or(e1Filter.get, e2Filter.get))
      } else {
        None
      }

    case eqExpr: EqualTo =>
      var e1 = eqExpr.getLeft
      var e2 = eqExpr.getRight
      if (eqExpr.getLeft.isInstanceOf[Literal]) {
        e1 = eqExpr.getRight
        e2 = eqExpr.getLeft
      }
      // assume e2 is Literal and e1 is Column
      val column = e1.asInstanceOf[Column]
      val minColumn = new Column(column.name() + "." + MIN, column.dataType())
      val maxColumn = new Column(column.name() + "." + MAX, column.dataType())
      // TODO extend data type
        Some(new And(new LessThanOrEqual(minColumn, e2),
          new GreaterThanOrEqual(maxColumn, e2)))

    // TODO: ...
    // Unknown expression type... can't use it for data skipping.
    case _ => None
  }

  object SkippingEligibleLiteral {
    def unapply(arg: Literal): Option[Expression] = {
      if (isEligibleDataType(arg.dataType)) Some(Literal.True) else None
    }

    def isEligibleDataType(dt: DataType): Boolean = dt match {
      // TODO case _: LongType | DateType | TimestampType | StringType => true
      case _ => false
    }
  }

  private def buildColumnStats(
      tableSchema: StructType,
      columnStats: String): StructType = {

    val jsonMap = JsonUtils.fromJson[Map[String, String]](columnStats)
    var res = new StructType()
    tableSchema match {
      case subSchema: StructType =>
        subSchema.getFields.foreach { x =>
          val subColumnStats = jsonMap.getOrElse(x.getName, null)
          res.add(buildColumnStats(subSchema, subColumnStats))
        }


    }
    val t1 = tableSchema.get(columnStats).getDataType match {
      case y: StructType => buildColumnStats(_, columnStats, stringStack)
      case z: StringType => new StructType({(StructField) })
    }
    var columnStatsStore = new StructType
    tableSchema.getFields.foreach { field =>
      columnStatsStore.add(

      )
    }


    columnStatsStore
  }

  /*
  private def verifyStatsForFilter(
      referencedStats: Set[StatsColumn],
      addFile: AddFile,
      metadata: Metadata): Expression = {
    val statsSchema = JsonUtils.fromJson[AddFileStats](addFile.stats)
    /* TODO parse more value type
    statsSchema.maxValues.map { x =>
      val fieldName = x._1
      val fieldDataType = metadata.getSchema.get(fieldName).getDataType
      fieldDataType.fromJson()
      val fieldValue = JsonUtils.convertValueFromObject[fieldDataType.type](x._2)
    }
     */
    referencedStats.map { stat =>
      val columnName = stat.pathToColumn.head // TODO: support nested column later
      val nullCount: Long = statsSchema.nullCount.getOrElse(columnName, null)
      val numRecords: Long = statsSchema.numRecords
    }


    /*
    referencedStats.flatMap { stat => stat match {
      case StatsColumn(MIN, _) | StatsColumn(MAX, _) =>
        Seq(stat, StatsColumn(NULL_COUNT, stat.pathToColumn), StatsColumn(NUM_RECORDS))
      case _ =>
        Seq(stat)
    }}.map {
      case stat @ (StatsColumn(MIN, _) | StatsColumn(MAX, _)) =>
        // A usable MIN or MAX stat must be non-NULL, unless the column is provably all-NULL
        //
        // NOTE: We don't care about NULL/missing NULL_COUNT and NUM_RECORDS here, because the
        // separate NULL checks we emit for those columns will force the overall validation
        // predicate conjunction to FALSE in that case -- AND(FALSE, <anything>) is FALSE.
        new Or(getStatsColumnOpt(stat.statType, addFile, stat.pathToColumn)
        .getOrElse(Literal.ofNull(new BooleanType)),
          new EqualTo(getStatsColumnOpt(NULL_COUNT, addFile, stat.pathToColumn)
          .getOrElse(Literal.ofNull(new BooleanType)),
            getStatsColumnOpt(NUM_RECORDS, addFile).getOrElse(Literal.ofNull(new BooleanType))))
      case stat =>
        // Other stats, such as NULL_COUNT and NUM_RECORDS stat, merely need to be non-NULL
        getStatsColumnOpt(stat.statType, addFile, stat.pathToColumn)
        .getOrElse(Literal.ofNull(new BooleanType))
    }.reduceLeftOption(new And(_, _)).getOrElse(Literal.ofNull(new BooleanType))
     */
  }
 */
}
