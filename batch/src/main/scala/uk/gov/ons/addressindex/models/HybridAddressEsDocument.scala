package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

case class HybridAddressEsDocument(
  uprn: Long,
  postcodeIn: String,
  postcodeOut: String,
  parentUprn: Long,
  relatives: Seq[Map[String, Any]],
  lpi: Seq[Map[String, Any]],
  paf: Seq[Map[String, Any]],
  crossRefs: Seq[Map[String, String]],
  classificationCode: Option[String]
)

object HybridAddressEsDocument extends EsDocument {

  def rowToLpi(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(0),
    "postcodeLocator" -> row.getString(1),
    "addressBasePostal" -> row.getString(2),
    "location" -> row.get(3),
    "easting" -> row.getFloat(4),
    "northing" -> row.getFloat(5),
    "parentUprn" -> (if (row.isNullAt(6)) null else row.getLong(6)),
    "multiOccCount" -> row.getShort(7),
    "blpuLogicalStatus" -> row.getByte(8),
    "localCustodianCode" -> row.getShort(9),
    "rpc" -> row.getByte(10),
    "organisation" -> row.getString(11),
    "legalName" -> row.getString(12),
    "usrn" -> row.getInt(13),
    "lpiKey" -> row.getString(14),
    "paoText" -> row.getString(15),
    "paoStartNumber" -> (if (row.isNullAt(16)) null else row.getShort(16)),
    "paoStartSuffix" -> row.getString(17),
    "paoEndNumber" -> (if (row.isNullAt(18)) null else row.getShort(18)),
    "paoEndSuffix" -> row.getString(19),
    "saoText" -> row.getString(20),
    "saoStartNumber" -> (if (row.isNullAt(21)) null else row.getShort(21)),
    "saoStartSuffix" -> row.getString(22),
    "saoEndNumber" -> (if (row.isNullAt(23)) null else row.getShort(23)),
    "saoEndSuffix" -> row.getString(24),
    "level" -> row.getString(25),
    "officialFlag" -> row.getString(26),
    "lpiLogicalStatus" -> row.getByte(27),
    "usrnMatchIndicator" -> row.getByte(28),
    "language" -> row.getString(29),
    "streetDescriptor" -> row.getString(30),
    "townName" -> row.getString(31),
    "locality" -> row.getString(32),
    "streetClassification" -> (if (row.isNullAt(33)) null else row.getByte(33)),
    "lpiStartDate" -> row.getDate(34),
    "lpiLastUpdateDate" -> row.getDate(35),
    "lpiEndDate" -> row.getDate(36),
    "nagAll" ->  concatNag(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24), row.getString(22), row.getString(20), row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19), row.getString(15), row.getString(30),
      row.getString(31), row.getString(32), row.getString(1)
    ),
    "mixedNag" -> generateFormattedNagAddress(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      row.getString(22),
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24), row.getString(20), row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19), row.getString(15), row.getString(30), row.getString(32), row.getString(31), row.getString(1)
    )
  )

  def rowToPaf(row: Row): Map[String, Any] = Map(
    "recordIdentifier" -> row.getByte(0),
    "changeType" -> Option(row.getString(1)).getOrElse(""),
    "proOrder" -> row.getLong(2),
    "uprn" -> row.getLong(3),
    "udprn" -> row.getInt(4),
    "organisationName" -> Option(row.getString(5)).getOrElse(""),
    "departmentName" -> Option(row.getString(6)).getOrElse(""),
    "subBuildingName" -> Option(row.getString(7)).getOrElse(""),
    "buildingName" -> Option(row.getString(8)).getOrElse(""),
    "buildingNumber" -> (if (row.isNullAt(9)) null else row.getShort(9)),
    "dependentThoroughfare" -> Option(row.getString(10)).getOrElse(""),
    "thoroughfare" -> Option(row.getString(11)).getOrElse(""),
    "doubleDependentLocality" -> Option(row.getString(12)).getOrElse(""),
    "dependentLocality" -> Option(row.getString(13)).getOrElse(""),
    "postTown" -> Option(row.getString(14)).getOrElse(""),
    "postcode" -> Option(row.getString(15)).getOrElse(""),
    "postcodeType" -> Option(row.getString(16)).getOrElse(""),
    "deliveryPointSuffix" -> Option(row.getString(17)).getOrElse(""),
    "welshDependentThoroughfare" -> Option(row.getString(18)).getOrElse(""),
    "welshThoroughfare" -> Option(row.getString(19)).getOrElse(""),
    "welshDoubleDependentLocality" -> Option(row.getString(20)).getOrElse(""),
    "welshDependentLocality" -> Option(row.getString(21)).getOrElse(""),
    "welshPostTown" -> Option(row.getString(22)).getOrElse(""),
    "poBoxNumber" -> Option(row.getString(23)).getOrElse(""),
    "processDate" -> row.getDate(24),
    "startDate" -> row.getDate(25),
    "endDate" -> row.getDate(26),
    "lastUpdateDate" ->row.getDate(27),
    "entryDate" -> row.getDate(28),
    "pafAll" -> concatPaf(Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(10)).getOrElse(""),
      Option(row.getString(18)).getOrElse(""),
      Option(row.getString(11)).getOrElse(""),
      Option(row.getString(19)).getOrElse(""),
      Option(row.getString(6)).getOrElse(""),
      Option(row.getString(5)).getOrElse(""),
      Option(row.getString(7)).getOrElse(""),
      Option(row.getString(8)).getOrElse(""),
      Option(row.getString(12)).getOrElse(""),
      Option(row.getString(20)).getOrElse(""),
      Option(row.getString(13)).getOrElse(""),
      Option(row.getString(21)).getOrElse(""),
      Option(row.getString(14)).getOrElse(""),
      Option(row.getString(22)).getOrElse(""),
      Option(row.getString(15)).getOrElse("")),
    "mixedPaf" -> generateFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(10)).getOrElse(""),
      Option(row.getString(11)).getOrElse(""),
      Option(row.getString(6)).getOrElse(""),
      Option(row.getString(5)).getOrElse(""),
      Option(row.getString(7)).getOrElse(""),
      Option(row.getString(8)).getOrElse(""),
      Option(row.getString(12)).getOrElse(""),
      Option(row.getString(13)).getOrElse(""),
      Option(row.getString(14)).getOrElse(""),
      Option(row.getString(15)).getOrElse("")
    ),
    "mixedWelshPaf" -> generateWelshFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(18)).getOrElse(Option(row.getString(10)).getOrElse("")),
      Option(row.getString(19)).getOrElse(Option(row.getString(11)).getOrElse("")),
      Option(row.getString(6)).getOrElse(""),
      Option(row.getString(5)).getOrElse(""),
      Option(row.getString(7)).getOrElse(""),
      Option(row.getString(8)).getOrElse(""),
      Option(row.getString(20)).getOrElse(Option(row.getString(12)).getOrElse("")),
      Option(row.getString(21)).getOrElse(Option(row.getString(13)).getOrElse("")),
      Option(row.getString(22)).getOrElse(Option(row.getString(14)).getOrElse("")),
      Option(row.getString(15)).getOrElse("")
    )
  )

  def rowToHierarchy(row: Row): Map[String, Any] = Map(
    "level" -> row.getAs[String]("level"),
    "siblings" -> row.getAs[Array[Long]]("siblings"),
    "parents" -> row.getAs[Array[Long]]("parents")
  )

  def rowToCrossRef(row: Row): Map[String, String] = Map(
    "crossReference" -> row.getAs[String]("crossReference"),
    "source" -> row.getAs[String]("source")
  )
}
