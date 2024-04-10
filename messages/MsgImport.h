#ifndef _BIO_MSG_IMPORT_
#define _BIO_MSG_IMPORT_

#include "Message.h"

namespace msg {

// Errors
MSG_ERROR(ERROR_EMPTY_DB_NAME, 2001,
          "Empty database name in '$0'")

MSG_ERROR(ERROR_UNEXPECTED_TOKEN, 2002,
          "Unexpected token '$0' at line $1")

MSG_ERROR(ERROR_NODE_ID_NOT_FOUND, 2003,
          "Node id not found at line $1")

MSG_ERROR(ERROR_NODE_TYPE_CREATE_FAILED, 2004,
          "Node type creation failed")

MSG_ERROR(ERROR_EDGE_TYPE_CREATE_FAILED, 2005,
          "Edge type creation failed")

MSG_ERROR(ERROR_FAILED_TO_CREATE_NODE, 2006,
          "Impossible to create node at line $0")

MSG_ERROR(ERROR_NO_SOURCE_OR_NO_EDGE, 2007,
          "A source or target node for an edge has not been given at line $0")

MSG_ERROR(ERROR_IMPOSSIBLE_TO_CONVERT_ID, 2008,
          "Impossible to convert node id '$0' to a number, at line $1")

MSG_ERROR(ERROR_NODE_NOT_FOUND, 2009,
          "No node of id='$0' has been found, required at line $0")

MSG_ERROR(ERROR_FAILED_TO_CREATE_EDGE, 2010,
          "Impossible to create edge at line $0")

MSG_ERROR(ERROR_FAILED_PARSE_GML_COMMAND, 2011,
          "Impossible to parse GML command at line $0")

MSG_ERROR(ERROR_NEO4J_CANNOT_FIND_ARCHIVE, 2012,
          "Failed to find Neo4j archive '$0'")

MSG_ERROR(ERROR_NEO4J_PROBLEM_WITH_NODETYPE, 2013,
          "The '$0' node type is invalid.")

MSG_ERROR(ERROR_NEO4J_CANNOT_DECOMPRESS_ARCHIVE, 2014,
          "Failed to decompress Neo4j archive '$0'")

MSG_ERROR(ERROR_NEO4J_FAILED_TO_START, 2015,
          "Failed to start Neo4j")

MSG_ERROR(ERROR_NEO4J_FAILED_TO_STOP, 2016,
          "Failed to stop Neo4j. Killing java process")

MSG_ERROR(ERROR_NEO4J_HTTP_REQUEST, 2017,
          "Error in request to Neo4j '$0'")

MSG_ERROR(ERROR_NEO4J_ALREADY_RUNNING, 2018,
          "Neo4j server is already running")

MSG_ERROR(ERROR_NEO4J_DB_DIR_INCORRECT, 2019,
          "The neo4j db dir ($0) should contain a dumped db file")

MSG_ERROR(ERROR_NEO4J_BAD_CURL_REQUEST, 2020,
          "Problem with the curl request: $0")

MSG_ERROR(ERROR_NEO4J_TYPE_NOT_HANDLED, 2021,
          "The '$0' type is not handled.")

MSG_ERROR(ERROR_JSON_FAILED_TO_PARSE, 2022,
          "Failed to parse JSON. Error: '$0'")

MSG_ERROR(ERROR_JSON_BAD_FILE, 2023,
          "Problem with the json file: $0")

MSG_ERROR(ERROR_JSON_INCORRECT_TYPE, 2024,
          "During JSON parsing, value '$0' (PropertyType: '$1') could not be casted from '$2' to '$3'")


// Warnings
MSG_WARNING(WARNING_JSON_INCORRECT_TYPE, 2025,
            "During JSON parsing, value '$0' of type '$1' was interpreted as '$2")



// Infos
MSG_INFO(INFO_COPY_DB, 2026,
         "Copy Neo4J database from '$0' to '$1'")

MSG_INFO(INFO_NEO4J_READY, 2027,
         "Neo4j server is ready")

MSG_INFO(INFO_NEO4J_READING_NODE_PROPERTIES, 2028,
         "Parsing node properties from Neo4j JSON dump")

MSG_INFO(INFO_NEO4J_READING_NODES, 2029,
         "Parsing nodes from Neo4j JSON dump")

MSG_INFO(INFO_NEO4J_READING_EDGE_PROPERTIES, 2030,
         "Parsing edge properties from Neo4j JSON dump")

MSG_INFO(INFO_NEO4J_READING_EDGES, 2031,
         "Parsing edges from Neo4j JSON dump")

MSG_INFO(INFO_NEO4J_READING_STATS, 2032,
         "Parsing stats from Neo4j JSON dump")

MSG_INFO(INFO_NEO4J_NODE_COUNT, 2033,
         "Database has $0 nodes")

MSG_INFO(INFO_NEO4J_EDGE_COUNT, 2034,
         "Database has $0 edges")

MSG_INFO(INFO_NEO4J_NODE_PROP_ERROR_COUNT, 2035,
         "$0 node property errors occured during neo4j db load")

MSG_INFO(INFO_NEO4J_EDGE_PROP_ERROR_COUNT, 2036,
         "$0 edge property errors occured during neo4j db load")

MSG_INFO(INFO_NEO4J_NODE_PROP_WARNING_COUNT, 2037,
         "$0 node property warnings occured during neo4j db load")

MSG_INFO(INFO_NEO4J_EDGE_PROP_WARNING_COUNT, 2038,
         "$0 edge property warnings occured during neo4j db load")

MSG_INFO(INFO_NEO4J_UNSUPPORTED_NODE_PROP_COUNT, 2039,
         "$0 unsupported node properties were found during neo4j db load")

MSG_INFO(INFO_NEO4J_UNSUPPORTED_EDGE_PROP_COUNT, 2040,
         "$0 unsupported edge properties were found during neo4j db load")

MSG_INFO(INFO_NEO4J_ILLFORMED_NODE_PROP_COUNT, 2041,
         "$0 illformed node properties were found during neo4j db load")

MSG_INFO(INFO_NEO4J_ILLFORMED_EDGE_PROP_COUNT, 2042,
         "$0 illformed edge properties were found during neo4j db load")

MSG_INFO(INFO_NEO4J_HTTP_REQUEST, 2043,
         "Sending Neo4j request: '$0'")

MSG_INFO(INFO_NEO4J_STOPPING, 2044,
         "Stopping Neo4j")

MSG_INFO(INFO_NEO4J_DECOMPRESS_ARCHIVE, 2045,
         "Decompressing Neo4j archive in $0")

MSG_INFO(INFO_NEO4J_CLEAN_SETUP, 2046,
         "Clean Neo4j setup")

MSG_INFO(INFO_NEO4J_STARTING, 2047,
         "Starting Neo4j at $0")

MSG_INFO(INFO_NEO4J_WARMING_UP, 2048,
         "Waiting for Neo4j to warmup")

MSG_INFO(INFO_NEO4J_IMPORT_DUMP_FILE, 2049,
         "Importing neo4j dump file: '$0'")

MSG_INFO(INFO_NEO4J_IMPORT_JSON_FILES, 2050,
         "Importing neo4j json files from dir: '$0'")

MSG_INFO(INFO_NEO4J_PARSED_NODE_COUNT, 2051,
         "Parsed $0 nodes")

MSG_INFO(INFO_NEO4J_PARSED_EDGE_COUNT, 2052,
         "Parsed $0 edges")

MSG_INFO(INFO_JSON_DISPLAY_NODES_STATUS, 2053,
         "Parsing nodes. Currently: $0/$1")

MSG_INFO(INFO_JSON_DISPLAY_EDGES_STATUS, 2054,
         "Parsing edges. Currently: $0/$1")

MSG_ERROR(ERROR_IMPORT_NO_PATH_GIVEN, 2055,
          "Please give the path of a Neo4J dump file with the -neo4j option, "
          "a json dump folder (obtained from a previous bioimport run) with the -jsonNeo4j option, "
          "or a path to a GML file with the -gml option")

MSG_ERROR(ERROR_IMPORT_NET_APPLIED_WITH_WRONG_ORDER, 2056,
          "Please precede the \"-net\" argument by an import option "
          "(such as -neo4j, -json-neo4j, -gml, ...)")

MSG_ERROR(ERROR_NETWORK_ALREADY_EXISTS, 2057,
          "A network with the same name ($0) already exists")

MSG_WARNING(WARNING_COULD_NOT_DEDUCE_NETWORK_NAME, 2058,
          "Could not deduce a name for the network from the path given ($0). Provide one with "
          "the '-net' option. Falling back to the full path")

MSG_ERROR(ERROR_SOURCE_NETWORK_NOT_SPECIFIED, 2059,
          "Trying to link but missing infos on the source network ($0)")

MSG_ERROR(ERROR_TARGET_NETWORK_NOT_SPECIFIED, 2060,
          "Trying to link but missing infos on the target network ($0)")

MSG_ERROR(ERROR_SOURCE_NETWORK_NOT_FOUND, 2061,
          "Trying to link to a source network that does not exist ($0)")

MSG_ERROR(ERROR_TARGET_NETWORK_NOT_FOUND, 2062,
          "Trying to link to a target network that does not exist ($0)")

MSG_ERROR(ERROR_CSV_DUPLICATE_IN_HEADER, 2063,
          "Encountered a duplicate value in header: '$0'")

MSG_ERROR(ERROR_CSV_INVALID_PRIMARY_KEY, 2064,
          "Specified primary column is invalid: '$0'")

MSG_ERROR(ERROR_CSV_TOO_MANY_ENTRIES, 2065,
          "Line $0 has too many cells")

MSG_ERROR(ERROR_CSV_MISSING_ENTRY, 2066,
          "Line $0 has missing cells")

MSG_ERROR(ERROR_IMPORT_PRIMARY_KEY_APPLIED_WITH_WRONG_ORDER, 2067,
          "Please precede the \"-primary-key\" argument by a \"-csv\" option")

MSG_ERROR(ERROR_URL_NOT_PROVIDED, 2068,
          "Please precede the url argument by the \"-neo4j-url\" option")

MSG_ERROR(ERROR_CSV_REDEFINITION_OF_NODE, 2069,
          "Line $0: node of type '$1' was already registered as '$2'")

MSG_ERROR(ERROR_JSON_CONTAINS_ERROR, 2070,
          "The json file contains some errors")

}

#endif
