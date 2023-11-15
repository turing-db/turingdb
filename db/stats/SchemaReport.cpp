#include "SchemaReport.h"

#include "Report.h"

#include "Network.h"
#include "DB.h"
#include "NodeType.h"
#include "EdgeType.h"
#include "PropertyType.h"

#include "TimerStat.h"

#include "BioLog.h"
#include "MsgDB.h"

using namespace db;
using namespace Log;

namespace {

void reportDBEntityType(const DBEntityType* entityType,
                        std::ostream& stream) {
    for (const PropertyType* propType : entityType->propertyTypes()) {
        stream << propType->getName().toStdString()
               << " : "
               << propType->getValueType().toString()
               << '\n';
    }
}

}

SchemaReport::SchemaReport(const Path& reportsDir, const DB* db)
    : _reportsDir(reportsDir), _db(db)
{
}

SchemaReport::~SchemaReport() {
}

void SchemaReport::writeReport() {
    TimerStat timerStat("Generating DB schema report");

    const auto reportPath = _reportsDir/"schema.rpt";

    BioLog::log(msg::INFO_DB_GENERATING_SCHEMA_REPORT() << reportPath.string());

    Report report(reportPath, "Database Schema Report");

    auto& stream = report.getStream();

    // Networks
    stream << "==== Networks ====\n";
    for (const Network* net: _db->networks()) {
        stream << net->getName().toStdString();
        stream << " (" << net->nodes().size() << " nodes)\n";
    }

    stream << '\n';

    // Node types
    stream << "==== Node Types ====\n";
    for (const NodeType* nodeType : _db->nodeTypes()) {
        stream << nodeType->getName().toStdString() << '\n';
    }

    stream << '\n';

    for (const NodeType* nodeType : _db->nodeTypes()) {
        stream << "=== " << nodeType->getName().toStdString() << " ===\n";
        reportDBEntityType(nodeType, stream);
        stream << '\n';
    }

    stream << '\n';

    // Edge types
    stream << "==== Edge Types ====\n";
    for (const EdgeType* edgeType : _db->edgeTypes()) {
        stream << edgeType->getName().toStdString() << '\n';
    }

    stream << '\n';

    for (const EdgeType* edgeType : _db->edgeTypes()) {
        stream << "=== " << edgeType->getName().toStdString() << " ===\n";
        stream << "FROM:";
        for (const NodeType* source : edgeType->sourceTypes()) {
            stream << ' ' << source->getName().toStdString();
        }
        stream << "\nTO:";
        for (const NodeType* target : edgeType->targetTypes()) {
            stream << ' ' << target->getName().toStdString();
        }
        stream << '\n';
        reportDBEntityType(edgeType, stream);
        stream << '\n';
    }
}
