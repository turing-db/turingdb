#ifndef _BIO_MSG_IMPORT_
#define _BIO_MSG_IMPORT_

#include "Message.h"

namespace msg {

MSG_ERROR(ERROR_IMPORT_FAILED_FIND_NEO4J_ARCHIVE, 2001,
    "Failed to find Neo4j archive '$0'")

MSG_INFO(INFO_DECOMPRESS_NEO4J_ARCHIVE, 2002,
    "Decompressing Neo4j archive in $0")

MSG_ERROR(ERROR_IMPORT_FAILED_DECOMPRESS_NEO4J_ARCHIVE, 2003,
    "Failed to decompress Neo4j archive '$0'")

MSG_INFO(INFO_CLEAN_NEO4J_SETUP, 2004,
    "Clean Neo4j setup")

MSG_INFO(INFO_STARTING_NEO4J, 2005,
    "Starting Neo4j at $0")

MSG_ERROR(ERROR_FAILED_TO_START_NEO4J, 2006,
    "Failed to start Neo4j")

MSG_INFO(INFO_STOPPING_NEO4J, 2007,
    "Stopping Neo4j")

MSG_ERROR(ERROR_FAILED_TO_STOP_NEO4J, 2008,
    "Failed to stop Neo4j")

MSG_ERROR(ERROR_EMPTY_DB_NAME, 2009,
    "Empty database name in '$0'")

MSG_INFO(INFO_COPY_DB, 2010,
    "Copy Neo4J database from '$0' to '$1'")

MSG_INFO(INFO_SEND_NEO4J_HTTP_REQUEST, 2011,
    "Sending HTTP request to Neo4j")

MSG_ERROR(ERROR_NEO4J_HTTP_REQUEST, 2012,
    "Error in request to Neo4j '$0'")

MSG_INFO(INFO_NEO4J_WAIT_WARMUP, 2013,
    "Waiting $0 seconds for Neo4j to warmup")

MSG_ERROR(ERROR_FAILED_TO_PARSE_JSON, 2014,
    "Failed to parse JSON file '$0'")

MSG_INFO(INFO_EXTRACTING_ENTITIES_FROM_NEO4J_DUMP, 2015,
    "Extracting nodes and relations from Neo4j dump")

MSG_ERROR(ERROR_UNEXPECTED_TOKEN, 2016,
          "Unexpected token '$0' at line $1")

MSG_ERROR(ERROR_NODE_ID_NOT_FOUND, 2017,
          "Node id not found at line $1")

MSG_ERROR(ERROR_NODE_TYPE_CREATE_FAILED, 2018,
          "Node type creation failed")

MSG_ERROR(ERROR_EDGE_TYPE_CREATE_FAILED, 2018,
          "Edge type creation failed")

MSG_ERROR(ERROR_FAILED_TO_CREATE_NODE, 2019,
          "Impossible to create node at line $0")

MSG_ERROR(ERROR_NO_SOURCE_OR_NO_EDGE, 2020,
          "A source or target node for an edge has not been given at line $0")

MSG_ERROR(ERROR_IMPOSSIBLE_TO_CONVERT_ID, 2021,
          "Impossible to convert node id '$0' to a number, at line $1")

MSG_ERROR(ERROR_NODE_NOT_FOUND, 2022,
          "No node of id='$0' has been found, required at line $0")

MSG_ERROR(ERROR_FAILED_TO_CREATE_EDGE, 2023,
          "Impossible to create edge at line $0")

MSG_ERROR(ERROR_FAILED_PARSE_GML_COMMAND, 2024,
          "Impossible to parse GML command at line $0")

}

#endif
