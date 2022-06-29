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

import scala.jdk.CollectionConverters._

import io.delta.standalone.actions.Metadata
import io.delta.standalone.data.RowRecord
import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, IsNull, Literal, Not, Or}
import io.delta.standalone.types.{BooleanType, DataType, StringType, StructField, StructType, TimestampType}

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay}
import io.delta.standalone.internal.data.{PartitionRowRecord, RowParquetRecordImpl}
import io.delta.standalone.internal.util.{DataTypeParser, JsonUtils, PartitionUtils}

private [stats] case class StatsColumn(
    statType: String,
    pathToColumn: Seq[String] = Nil)

private [stats] case class ColumnStatsPredicate(
    expr: Expression,
    referencedStats: Set[StatsColumn])

private [stats] case class AddFileStats(
    numRecords: Long,
    nullCount: Map[String, Long],
    minValues: Map[String, Object],
    maxValues: Map[String, Object])

/**
 * An implementation of [[io.delta.standalone.DeltaScan]] that filters files and only returns
 * those that match the [[getPushedPredicate]].
 *
 * If the pushed predicate is empty, then all files are returned.
 */
final private[internal] class FilteredDeltaScanImpl(
    replay: MemoryOptimizedLogReplay,
    expr: Expression,
    metadata: Metadata) extends DeltaScanImpl(replay) {

  private val partitionColumns = metadata.getPartitionColumns.asScala.toSeq

  private val (metadataConjunction, dataConjunction) =
    PartitionUtils.splitMetadataAndDataPredicates(expr, partitionColumns)

  override protected def accept(addFile: AddFile): Boolean = {
    if (metadataConjunction.isEmpty) return true

    val partitionSchema = new StructType(metadata.getSchema.getFields.filter()

    val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
    val result = metadataConjunction.get.eval(partitionRowRecord).asInstanceOf[Boolean]

    // parse min max in column stats
    // TODO: column stats might be missing

    // make conjunctions by min/max and dataConjunction
    val columnStatsPredicate = constructDataFilters(dataConjunction.get).getOrElse {
      return result
    }

    // eval, get Boolean result, and return
    val dataRowRecord = new RowParquetRecordImpl(partitionSchema, addFile.partitionValues)
    // TODO: instantiate new row record by min max column value, maybe make a new record?

    val finalColumnStatsPredicate = new Or(columnStatsPredicate.expr,
      new Not(verifyStatsForFilter(columnStatsPredicate.referencedStats, addFile, metadata)))
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
    Option[ColumnStatsPredicate] = expression match {
    case andExpr: And =>
      val e1 = andExpr.getLeft
      val e2 = andExpr.getRight
      val e1Filter = constructDataFilters(e1)
      val e2Filter = constructDataFilters(e2)
      if (e1Filter.isDefined && e2Filter.isDefined) {
        Some(ColumnStatsPredicate(
          new And(e1Filter.get.expr, e2Filter.get.expr),
          e1Filter.get.referencedStats ++ e2Filter.get.referencedStats))
      } else if (e1Filter.isDefined) {
        e1Filter
      } else {
        e2Filter  // possibly None
      }

    case notAndExpr: Not =>
      notAndExpr.getChild match {
        case andExpr: And =>
          val e1 = andExpr.getLeft
          val e2 = andExpr.getRight

          constructDataFilters(new Or(new Not(e1), new Not(e2)))
      }


    case orExpr: Or =>
      val e1 = orExpr.getLeft
      val e2 = orExpr.getRight
      val e1Filter = constructDataFilters(e1)
      val e2Filter = constructDataFilters(e2)
      if (e1Filter.isDefined && e2Filter.isDefined) {
        Some(ColumnStatsPredicate(
          new Or(e1Filter.get.expr, e2Filter.get.expr),
          e1Filter.get.referencedStats ++ e2Filter.get.referencedStats))
      } else {
        None
      }
    // TODO: ...
    // Unknown expression type... can't use it for data skipping.
    case _ => None
  }

  /**
   * Returns an expression to access the given statistics for a specific column, or None if that
   * stats column does not exist.
   *
   * @param statType One of the fields declared by trait `UsesMetadataFields`
   * @param pathToColumn The components of the nested column name to get stats for.
   */
  protected def getStatsColumnOpt(
      statType: String,
      addFile: AddFile,
      pathToColumn: Seq[String] = Nil): Boolean = {
    // If the requested stats type doesn't even exist, just return None right away. This can
    // legitimately happen if we have no stats at all, or if column stats are disabled (in which
    // case only the NUM_RECORDS stat type is available).
    val statsSchema = new StructType(JsonUtils.fromJson[Array[StructField]](addFile.stats))
    if (!statsSchema.getFields.exists(_.getName == statType)) {
      return None
    }

    val result = Literal.of("stats")


  }

  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"


  private def verifyStatsForFilter(
      referencedStats: Set[StatsColumn],
      addFile: AddFile,
      metadata: Metadata): Expression = {
    val statsSchema = JsonUtils.fromJson[AddFileStats](addFile.stats)
    statsSchema.maxValues.map { x =>
      val fieldName = x._1
      val fieldDataType = metadata.getSchema.get(fieldName).getDataType
      fieldDataType.fromJson()
      val fieldValue = JsonUtils.convertValueFromObject[fieldDataType.type](x._2)
    }



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
        new Or(getStatsColumnOpt(stat.statType, addFile, stat.pathToColumn).getOrElse(Literal.ofNull(new BooleanType)),
          // TODO check all getOrElse
          new EqualTo(getStatsColumnOpt(NULL_COUNT, addFile, stat.pathToColumn).getOrElse(Literal.ofNull(new BooleanType)),
            getStatsColumnOpt(NUM_RECORDS, addFile).getOrElse(Literal.ofNull(new BooleanType))))
      case stat =>
        // Other stats, such as NULL_COUNT and NUM_RECORDS stat, merely need to be non-NULL
        getStatsColumnOpt(stat.statType, addFile, stat.pathToColumn).getOrElse(Literal.ofNull(new BooleanType))
    }.reduceLeftOption(new And(_, _)).getOrElse(Literal.ofNull(new BooleanType))
  }
}
