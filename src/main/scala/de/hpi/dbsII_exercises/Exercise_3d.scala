package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

/***
 * Bestimmen Sie die Change-Signatures aller  Attribute. Die Change Signature ist eine sortierte Liste an Zeitpunkten,
 * zu denen viele Änderungen an dem Attribut stattfanden. Ein Zeitpunkt soll genau dann in der Change Signature eines Attributes A auftauchen,
 * wenn mindestens 100 entitities zu diesem Zeitpunkt im Attribut A ihren Wert geändert haben. Geben Sie für jede Change-Signature alle Attribute
 * (Identifiziert durch die Kombination aus tableID und attributeName) zurück, die diese Change-Signature aufweisen.
 * @param spark - the spark context
 * @param changeRecords - the input spark dataset containing the change records
 */
class Exercise_3d(spark: SparkSession, changeRecords: Dataset[ChangeRecord]) {

  import spark.implicits._

  /***
   *
   * @return a map containing all change signatures that appear in the data. Format:
   *         Key: the change-signature, represented as a sorted sequence of Timestamps
   *         Value: a sequence of all attributes (represented as tuples in the form of (tableID,attrID) ), that belong to this change-signature
   */
  def execute():Map[Seq[Timestamp],Seq[(String,String)]] = {
    // Source: https://stackoverflow.com/a/29986300
    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }
    changeRecords
      .groupByKey(changeRecord => (changeRecord.tableID, changeRecord.attributeName, changeRecord.timestamp))
      .mapValues(changeRecord => changeRecord.entityID)
      .mapGroups((key, entityIt) => {
        (key._1, key._2, key._3, entityIt.toSeq)
      })
      .filter(_._4.distinct.size >= 100)
      .groupByKey(item => (item._1, item._2))
      .mapValues(_._3)
      .mapGroups((key, timestampIt) => {
        (key._1, key._2, timestampIt.toSeq.sorted)
      })
      .map(item => (item._1, item._2, item._3))
      .groupByKey(_._3)
      .mapValues(item => (item._1, item._2))
      .mapGroups(_ -> _.toSeq).collect.toMap
  }

}
