#include "Message.h"

namespace msg {

MSG_INFO(INFO_DB_DUMPING_DATABASE, 5001,
         "Saving Turing DB to $0")

MSG_INFO(INFO_DB_LOADING_DATABASE, 5002,
         "Loading Turing DB from $0")

MSG_INFO(INFO_DB_LOADING_STRING_INDEX, 5003,
         "Loading string index...")

MSG_INFO(INFO_DB_LOADING_TYPE_INDEX, 5004,
         "Loading type index...")

MSG_INFO(INFO_DB_LOADING_ENTITY_INDEX, 5005,
         "Loading entity index...")

MSG_INFO(ERROR_DB_DUMPING_DATABASE, 5006,
         "Could not dump Turing DB to $0")

MSG_INFO(ERROR_DB_LOADING_DATABASE, 5007,
         "Could not load Turing DB from $0")

MSG_FATAL(FATAL_INVALID_PROPERTY_DUMP, 5008,
         "Should not occur: Invalid property encountered while dumping db")

MSG_FATAL(FATAL_INVALID_PROPERTY_LOAD, 5009,
         "Should not occur: Invalid property encountered while loading db")

MSG_INFO(INFO_DB_GENERATING_SCHEMA_REPORT, 5010,
         "Generating database schema report in $0")

MSG_INFO(INFO_DB_DONE_LOADING_DATABASE, 5011,
         "Done loading Turing DB from $0")

}

