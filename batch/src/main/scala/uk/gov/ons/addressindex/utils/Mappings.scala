package uk.gov.ons.addressindex.utils

object Mappings {
  val hybrid =
    """
      {
        "settings": {
          "number_of_shards": 1,
          "number_of_replicas": 1,
          "analysis": {
            "analyzer": {
              "welsh_no_split_analyzer": {
                "tokenizer": "custom_keyword",
                "filter": [
                  "asciifolding"
                ]
              },
              "welsh_split_analyzer": {
                "tokenizer": "classic",
                "filter": [
                  "asciifolding"
                ]
              }
            },
            "tokenizer": {
              "custom_keyword": {
                "type": "keyword",
                "buffer_size": 128
              }
            }
          }
        },
        "mappings": {
          "address": {
            "properties": {
              "lpi": {
                "properties": {
                  "addressBasePostal": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "classificationCode": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "easting": {
                    "type": "float",
                    "index": "not_analyzed"
                  },
                  "location": {
                    "type": "geo_point",
                    "index": "not_analyzed"
                  },
                  "legalName": {
                    "type": "string",
                    "analyzer": "welsh_split_analyzer"
                  },
                  "level": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "locality": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "lpiLogicalStatus": {
                    "type": "byte",
                    "index": "not_analyzed"
                  },
                  "blpuLogicalStatus": {
                    "type": "byte",
                    "index": "not_analyzed"
                  },
                  "lpiKey": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "northing": {
                    "type": "float",
                    "index": "not_analyzed"
                  },
                  "officialFlag": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "organisation": {
                    "type": "string",
                    "analyzer": "welsh_split_analyzer"
                  },
                  "paoEndNumber": {
                    "type": "short",
                    "index": "not_analyzed"
                  },
                  "paoEndSuffix": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "paoStartNumber": {
                    "type": "short",
                    "index": "not_analyzed"
                  },
                  "paoStartSuffix": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "paoText": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "postcodeLocator": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "saoEndNumber": {
                    "type": "short",
                    "index": "not_analyzed"
                  },
                  "saoEndSuffix": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "saoStartNumber": {
                    "type": "short",
                    "index": "not_analyzed"
                  },
                  "saoStartSuffix": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "saoText": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "streetDescriptor": {
                    "type": "string",
                    "analyzer": "welsh_split_analyzer"
                  },
                  "townName": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "uprn": {
                    "type": "long",
                    "index": "not_analyzed"
                  },
                  "usrn": {
                    "type": "integer",
                    "index": "not_analyzed"
                  },
                  "parentUprn": {
                    "type": "long",
                    "index": "not_analyzed"
                  },
                  "multiOccCount": {
                    "type": "short",
                    "index": "not_analyzed"
                  },
                  "localCustodianCode": {
                    "type": "short",
                    "index": "not_analyzed"
                  },
                  "rpc": {
                    "type": "byte",
                    "index": "not_analyzed"
                  },
                  "usrnMatchIndicator": {
                    "type": "byte",
                    "index": "not_analyzed"
                  },
                  "language": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "streetClassification": {
                    "type": "byte",
                    "index": "not_analyzed"
                  },
                  "classScheme": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "crossReference": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "source": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "nagAll": {
                    "type": "string",
                    "analyzer": "welsh_split_analyzer"
                  },
                  "relatives": {
                    "type": "long",
                    "index": "no"
                  },
                  "lpiStartDate": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                    "index": "not_analyzed"
                  },
                  "lpiLastUpdateDate": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                    "index": "not_analyzed"
                  }
                }
              },
              "paf": {
                "properties": {
                  "buildingName": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "buildingNumber": {
                    "type": "short",
                    "index": "not_analyzed"
                  },
                  "changeType": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "deliveryPointSuffix": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "departmentName": {
                    "type": "string",
                    "analyzer": "welsh_split_analyzer"
                  },
                  "dependentLocality": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "dependentThoroughfare": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "doubleDependentLocality": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "endDate": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                    "index": "not_analyzed"
                  },
                  "entryDate": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                    "index": "not_analyzed"
                  },
                  "lastUpdateDate": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                    "index": "not_analyzed"
                  },
                  "organizationName": {
                    "type": "string",
                    "analyzer": "welsh_split_analyzer"
                  },
                  "poBoxNumber": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "postTown": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "postcode": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "postcodeType": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "proOrder": {
                    "type": "long",
                    "index": "not_analyzed"
                  },
                  "processDate": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                    "index": "not_analyzed"
                  },
                  "recordIdentifier": {
                    "type": "byte",
                    "index": "not_analyzed"
                  },
                  "startDate": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                    "index": "not_analyzed"
                  },
                  "subBuildingName": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "thoroughfare": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "udprn": {
                    "type": "integer",
                    "index": "not_analyzed"
                  },
                  "uprn": {
                    "type": "long",
                    "index": "not_analyzed"
                  },
                  "welshDependentLocality": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "welshDependentThoroughfare": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "welshDoubleDependentLocality": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "welshPostTown": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "welshThoroughfare": {
                    "type": "string",
                    "analyzer": "welsh_no_split_analyzer"
                  },
                  "pafAll": {
                    "type": "string",
                    "analyzer": "welsh_split_analyzer"
                  }
                }
              },
              "uprn": {
                "type": "long",
                "index": "not_analyzed"
              },
              "postcodeIn": {
                "type": "string",
                "index": "not_analyzed"
              },
              "postcodeOut": {
                "type": "string",
                "index": "not_analyzed"
              }
            }
          }
        }
      }
    """
}