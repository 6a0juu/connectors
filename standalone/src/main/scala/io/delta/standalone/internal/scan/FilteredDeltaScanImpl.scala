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
import io.delta.standalone.types.{DataType, StructField, StructType}

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay}
import io.delta.standalone.internal.data.{PartitionRowRecord, RowParquetRecordImpl}
import io.delta.standalone.internal.util.{JsonUtils, PartitionUtils}

private [stats] case class StatsColumn(
    statType: String,
    pathToColumn: Seq[String] = Nil)

private [stats] case class ColumnStatsPredicate(
    expr: Expression,
    referencedStats: Set[StatsColumn])

// private [stats] case class ColumnStats(minValues: collection.mutable.Map[String, ], maxValues: Long)

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
    val statsMap = JsonUtils.fromJson[StructType](addFile.stats)
    // TODO: change JSON path to minValue.col_name
    // TODO: column stats might be missing

    // make conjunctions by min/max and dataConjunction
    val columnStatsPredicate = constructDataFilters(dataConjunction.get).getOrElse {
      return result
    }

    // eval, get Boolean result, and return
    val dataRowRecord = new RowParquetRecordImpl(partitionSchema, addFile.partitionValues)
    // TODO: instantiate new row record by min max column value, maybe make a new record?

    val finalColumnStatsPredicate = new Or(columnStatsPredicate.expr,
      new Not(verifyStatsForFilter(columnStatsPredicate.referencedStats, addFile)))
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
      pathToColumn: Seq[String] = Nil): Option[Expression] = {
    // If the requested stats type doesn't even exist, just return None right away. This can
    // legitimately happen if we have no stats at all, or if column stats are disabled (in which
    // case only the NUM_RECORDS stat type is available).
    val statsSchema = new StructType(JsonUtils.fromJson[Array[StructField]](addFile.stats))
    if (!statsSchema.getFields.exists(_.getName == statType)) {
      return None
    }

    // Given a set of path segments in reverse order, e.g. column a.b.c is Seq("c", "b", "a"), we
    // use a foldRight operation to build up the requested stats column, by successively applying
    // each new path step against both the table schema and the stats schema. We can't use the stats
    // schema alone, because the caller-provided path segments use logical column names, while the
    // stats schema requires physical column names. Instead, we must step into the table schema to
    // extract that field's physical column name, and use the result to step into the stats schema.
    //
    // We use a three-tuple to track state. The traversal starts with the base column for the
    // requested stat type, the stats schema for the requested stat type, and the table schema. Each
    // step of the traversal emits the updated column, along with the stats schema and table schema
    // elements corresponding to that column.
    val initialState: Option[(Column, DataType, DataType)] =
    Some(("stats", statType.getField(statType),
      statsSchema.getFields.dataType,
      metadata.schema))
    pathToColumn
      .foldRight(initialState) {
        // NOTE: Only match on StructType, because we cannot traverse through other DataTypes.
        case (fieldName, Some((statCol, statsSchema: StructType, tableSchema: StructType))) =>
          // First try to step into the table schema
          val tableFieldOpt = tableSchema.findNestedFieldIgnoreCase(Seq(fieldName))

          // If that worked, try to step into the stats schema, using its its physical name
          val statsFieldOpt = tableFieldOpt
            .map(DeltaColumnMapping.getPhysicalName)
            .filter(physicalFieldName => statsSchema.getFields.exists(_.getName == physicalFieldName))
            .map(statsSchema(_))
          // TODO: since column mapping is not implemented in standalone, this will be deleted.

          // If all that succeeds, return the new stats column and the corresponding data types.
          statsFieldOpt.map(statsField =>
            (statCol.getField(statsField.name), statsField.dataType, tableFieldOpt.get.dataType))

        // Propagate failure if the above match failed (or if already None)
        case _ => None
      }
      // Filter out non-leaf columns -- they lack stats so skipping predicates can't use them.
      .filterNot(_._2.isInstanceOf[StructType])
      .map {
        case (statCol, TimestampType, _) if statType == MAX =>
          // SC-22824: For timestamps, JSON serialization will truncate to milliseconds. This means
          // that we must adjust 1 millisecond upwards for max stats, or we will incorrectly skip
          // records that differ only in microsecond precision. (For example, a file containing only
          // 01:02:03.456789 will be written with min == max == 01:02:03.456, so we must consider it
          // to contain the range from 01:02:03.456 to 01:02:03.457.)
          //
          // There is a longer term task SC-22825 to fix the serialization problem that caused this.
          // But we need the adjustment in any case to correctly read stats written by old versions.
          new Column(Cast(TimeAdd(statCol.expr, oneMillisecondLiteralExpr), TimestampType))
        case (statCol, _, _) =>
          statCol
      }
  }

  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"


  private def verifyStatsForFilter(referencedStats: Set[StatsColumn], addFile: AddFile): Expression = {
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
        new Or(getStatsColumnOpt(stat.statType, addFile, stat.pathToColumn).getOrElse(Literal.False),
          // TODO check all getOrElse
          new EqualTo(getStatsColumnOpt(NULL_COUNT, addFile, stat.pathToColumn).getOrElse(Literal.False),
            getStatsColumnOpt(NUM_RECORDS, addFile).getOrElse(Literal.False)))
      case stat =>
        // Other stats, such as NULL_COUNT and NUM_RECORDS stat, merely need to be non-NULL
        getStatsColumnOpt(stat.statType, addFile, stat.pathToColumn).getOrElse(Literal.False)
    }.reduceLeftOption(new And(_, _)).getOrElse(Literal.True)
  }
}
