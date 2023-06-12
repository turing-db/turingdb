#include "JsonParser.h"
#include "BioLog.h"
#include "DB.h"
#include "MsgImport.h"
#include "Neo4j4JsonParser.h"

JsonParser::JsonParser()
    : _db(db::DB::create()),
      _neo4j4Parser(_db, _stats)
{
}

JsonParser::JsonParser(db::DB* db)
    : _db(db),
      _neo4j4Parser(_db, _stats)
{
}

bool JsonParser::parse(const std::string& data, Format format) {
    try {
        switch (format) {
            case Format::Neo4j4_Stats:
                return _neo4j4Parser.parseStats(data);

            case Format::Neo4j4_NodeProperties:
                return _neo4j4Parser.parseNodeProperties(data);

            case Format::Neo4j4_Nodes:
                Log::BioLog::log(msg::INFO_JSON_DISPLAY_NODES_STATUS()
                                 << _stats.parsedNodes << _stats.nodeCount);
                return _neo4j4Parser.parseNodes(data);

            case Format::Neo4j4_EdgeProperties:
                return _neo4j4Parser.parseEdgeProperties(data);

            case Format::Neo4j4_Edges:
                Log::BioLog::log(msg::INFO_JSON_DISPLAY_EDGES_STATUS()
                                 << _stats.parsedEdges << _stats.edgeCount);
                return _neo4j4Parser.parseEdges(data);
        }

    } catch (const std::exception& e) {
        Log::BioLog::log(msg::ERROR_JSON_FAILED_TO_PARSE() << e.what());
    }

    return false;
}

db::DB* JsonParser::getDB() const {
    return _db;
}
