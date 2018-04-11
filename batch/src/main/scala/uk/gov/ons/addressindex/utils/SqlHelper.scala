package uk.gov.ons.addressindex.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import uk.gov.ons.addressindex.models.{CrossRefDocument, HierarchyDocument, HybridAddressEsDocument}

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, lpi: DataFrame, organisation: DataFrame, classification: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame, historical: Boolean = true): DataFrame = {

    val blpuTable =
      if (historical) {
        SparkProvider.registerTempTable(blpu, "blpu")
      } else {
        val blpuNoHistory = SparkProvider.registerTempTable(blpu, "blpuNoHistory")
        val blpuNoHistoryDF = SparkProvider.sqlContext.sql(s"""SELECT b.* FROM $blpuNoHistory b WHERE b.logicalStatus != 8""")
        SparkProvider.registerTempTable(blpuNoHistoryDF, "blpu")
      }
    val organisationTable = SparkProvider.registerTempTable(organisation, "organisation")
    val classificationTable = SparkProvider.registerTempTable(classification, "classification")
    val lpiTable =
      if (historical) {
        SparkProvider.registerTempTable(lpi, "lpi")
      } else {
        val lpiNoHistory = SparkProvider.registerTempTable(lpi, "lpiNoHistory")
        val lpiNoHistoryDF = SparkProvider.sqlContext.sql(s"""SELECT l.* FROM $lpiNoHistory l WHERE l.logicalStatus != 8""")
        SparkProvider.registerTempTable(lpiNoHistoryDF, "lpi")
      }
    val streetTable = SparkProvider.registerTempTable(street, "street")
    val streetDescriptorTable = SparkProvider.registerTempTable(streetDescriptor, "street_descriptor")

    SparkProvider.sqlContext.sql(
      s"""SELECT
        $blpuTable.uprn,
        $blpuTable.postcodeLocator,
        $blpuTable.addressbasePostal as addressBasePostal,
        array($blpuTable.longitude, $blpuTable.latitude) as location,
        $blpuTable.xCoordinate as easting,
        $blpuTable.yCoordinate as northing,
        $blpuTable.parentUprn,
        $blpuTable.multiOccCount,
        $blpuTable.logicalStatus as blpuLogicalStatus,
        $blpuTable.localCustodianCode,
        $blpuTable.rpc,
        $organisationTable.organisation,
        $organisationTable.legalName,
        $classificationTable.classScheme,
        $classificationTable.classificationCode,
        $lpiTable.usrn,
        $lpiTable.lpiKey,
        $lpiTable.paoText,
        $lpiTable.paoStartNumber,
        $lpiTable.paoStartSuffix,
        $lpiTable.paoEndNumber,
        $lpiTable.paoEndSuffix,
        $lpiTable.saoText,
        $lpiTable.saoStartNumber,
        $lpiTable.saoStartSuffix,
        $lpiTable.saoEndNumber,
        $lpiTable.saoEndSuffix,
        $lpiTable.level,
        $lpiTable.officialFlag,
        $lpiTable.logicalStatus as lpiLogicalStatus,
        $lpiTable.usrnMatchIndicator,
        $lpiTable.language,
        $streetDescriptorTable.streetDescriptor,
        $streetDescriptorTable.townName,
        $streetDescriptorTable.locality,
        $streetTable.streetClassification,
        $lpiTable.startDate as lpiStartDate,
        $lpiTable.lastUpdateDate as lpiLastUpdateDate,
        $lpiTable.endDate as lpiEndDate
      FROM $blpuTable
      LEFT JOIN $organisationTable ON $blpuTable.uprn = $organisationTable.uprn
      LEFT JOIN $classificationTable ON $blpuTable.uprn = $classificationTable.uprn
      LEFT JOIN $lpiTable ON $blpuTable.uprn = $lpiTable.uprn
      LEFT JOIN $streetTable ON $lpiTable.usrn = $streetTable.usrn
      LEFT JOIN $streetDescriptorTable ON $streetTable.usrn = $streetDescriptorTable.usrn
      AND $lpiTable.language = $streetDescriptorTable.language""").na.fill("")
  }

  /**
    * Aggregates data forming lists of siblings and their parents per level of the hierarchy
    * (grouped by root uprn)
    * @param hierarchy hierarchy data
    * @return dataframe containing layers/levels of hierarchy
    */
  def aggregateHierarchyInformation(hierarchy: DataFrame): DataFrame ={
    val hierarchyTable = SparkProvider.registerTempTable(hierarchy, "hierarchy")

    SparkProvider.sqlContext.sql(
      s"""SELECT
            primaryUprn,
            thisLayer as level,
            collect_list(uprn) as siblings,
            collect_list(parentUprn) as parents
          FROM
            $hierarchyTable
          GROUP BY primaryUprn, thisLayer
       """
    )
  }

  def aggregateCrossRefInformation(crossRef: DataFrame): DataFrame = {
    val crossRefTable = SparkProvider.registerTempTable(crossRef, "crossRef")

    SparkProvider.sqlContext.sql(
      s"""SELECT
            uprn,
            crossReference,
            source
          FROM
            $crossRefTable
          GROUP BY uprn, crossReference, source
       """
    )
  }

  /**
    * Construct an RDD of hierarchical documents from the intial hierarchy data and the aggregated hierarchy data
    * @param hierarchy initial hierarchy data
    * @param aggregatedHierarchy aggregated hiearachy data
    * @return RDD of hierarchical documents containing information about relatives of a particular address
    */
  def constructHierarchyRdd(hierarchy: DataFrame, aggregatedHierarchy: DataFrame): RDD[HierarchyDocument] = {

    val hierarchyGroupedByPrimaryUprn = aggregatedHierarchy.rdd
      // The following code is a replacement for `groupBy(_.getLong(0))`, that works without additional shuffling (faster)
      .keyBy(row => row.getLong(0))
      .aggregateByKey(Seq.empty[Row])((acc: Seq[Row], row: Row) => acc :+ row, (acc1: Seq[Row], acc2: Seq[Row]) => acc1 ++ acc2)

    val hierarchyRdd = hierarchy.rdd.keyBy(row => row.getLong(1))

    hierarchyRdd.join(hierarchyGroupedByPrimaryUprn).map { case (_, (hierarchyRow: Row, relations: Iterable[Row])) =>
      HierarchyDocument.fromJoinData(hierarchyRow, relations)
    }
  }

  def constructCrossRefRdd(crossRef: DataFrame, aggregatedCrossRef: DataFrame): RDD[CrossRefDocument] = {

    val crossRefGroupedByUprn = aggregatedCrossRef.rdd.groupBy(row => row.getLong(0))
    val crossRefRdd = crossRef.rdd.keyBy(row => row.getLong(3))

    crossRefRdd.join(crossRefGroupedByUprn).map{ case (_, (crossRefRow: Row, crossRefs: Iterable[Row])) =>
        CrossRefDocument.fromJoinData(crossRefRow, crossRefs)
    }
  }

  /**
    * Constructs a hybrid index from nag and paf dataframes
    * We couldn't use Spark Sql because it does not contain `collect_list` until 2.0
    * Hive does not support aggregating complex types in the `collect_list` udf
    */
  def aggregateHybridIndex(paf: DataFrame, nag: DataFrame, hierarchy: RDD[HierarchyDocument], crossRef: RDD[CrossRefDocument], historical: Boolean = true): RDD[HybridAddressEsDocument] = {

    val nagWithKey = nag.rdd.keyBy(row => row.getLong(0))
    // If non-historical there could be zero lpis associated with the PAF record since historical lpis were filtered
    // out at the joinCsvs stage. These need to be removed.
    val pafWithKey =
      if (historical) {
        paf.rdd.keyBy(row => row.getLong(3))
      } else {
        paf.join(nag, Seq("uprn"), "leftsemi")
          .select("recordIdentifier","changeType","proOrder","uprn","udprn","organisationName","departmentName",
            "subBuildingName","buildingName","buildingNumber","dependentThoroughfare","thoroughfare",
            "doubleDependentLocality","dependentLocality","postTown","postcode","postcodeType","deliveryPointSuffix",
            "welshDependentThoroughfare","welshThoroughfare","welshDoubleDependentLocality","welshDependentLocality",
            "welshPostTown","poBoxNumber","processDate","startDate","endDate","lastUpdateDate","entryDate")
          .rdd.keyBy(row => row.getAs[Long]("uprn"))
      }

    val hierarchyWithKey = hierarchy.keyBy(document => document.uprn)
    val crossRefWithKey = crossRef.keyBy(document => document.uprn)

    // Following line will group rows in 2 groups: lpi and paf
    // The first element in each new row will contain `uprn` as the first key
    val groupedRdd = nagWithKey.cogroup(pafWithKey)

    val groupedRddWithHierarchyCrossRef = groupedRdd.leftOuterJoin(hierarchyWithKey).leftOuterJoin(crossRefWithKey)

    groupedRddWithHierarchyCrossRef.map {
      case (uprn, (((lpiArray, pafArray), hierarchyDocument), crossRefDocument)) =>

        val lpis = lpiArray.toSeq.map(HybridAddressEsDocument.rowToLpi)
        val pafs = pafArray.toSeq.map(HybridAddressEsDocument.rowToPaf)

        val lpiPostCode: Option[String] = lpis.headOption.flatMap(_.get("postcodeLocator").map(_.toString))
        val pafPostCode: Option[String] = pafs.headOption.flatMap(_.get("postcode").map(_.toString))

        val postCode = if (pafPostCode.isDefined) pafPostCode.getOrElse("")
        else lpiPostCode.getOrElse("")

        val splitPostCode = postCode.split(" ")
        val (postCodeOut, postCodeIn) =
          if (splitPostCode.size == 2 && splitPostCode(1).length == 3) (splitPostCode(0), splitPostCode(1))
          else ("", "")

        // fun fact: `null.asInstanceOf[Long]` is actually equal to `0l`
        val parentUprn = hierarchyDocument.map(_.parentUprn).getOrElse(0l)
        val relatives = hierarchyDocument.map(_.relations).getOrElse(Array())
        val crossRefs = crossRefDocument.map(_.crossRefs).getOrElse(Array())

        HybridAddressEsDocument(
          uprn,
          postCodeIn,
          postCodeOut,
          parentUprn,
          relatives,
          lpis,
          pafs,
          crossRefs
        )
    }
  }
}
