#include "PipeUtils.h"

#include <iostream>

#include <stdlib.h>
#include <spdlog/spdlog.h>

#include "DBServer.h"
#include "MemoryManagerStorage.h"
#include "SystemManager.h"
#include "JobSystem.h"
#include "Neo4j/Neo4JParserConfig.h"
#include "Neo4jImporter.h"

#include "DB.h"
#include "QueryInterpreter.h"
#include "InterpreterContext.h"
#include "QueryParams.h"
#include "QueryPlannerParams.h"
#include "DBView.h"
#include "DataPartBuilder.h"

#include "DBServerContext.h"
#include "DBServerProcessor.h"
#include "Server.h"

#include "Time.h"
#include "LogUtils.h"
#include "LogSetup.h"
#include "PerfStat.h"
#include "Panic.h"

using namespace db;
using namespace net;

PipeSample::PipeSample(const std::string& sampleName)
    : _sampleName(sampleName)
{
    LogSetup::setupLogFileBacked(sampleName + ".log");
    PerfStat::init(sampleName + ".perf");
    spdlog::set_level(spdlog::level::info);

    _jobSystem = std::make_unique<JobSystem>();
    _jobSystem->initialize();
    
    _server = std::make_unique<DBServer>(_serverConfig);
    _system = _server->getSystemManager();

    QueryInterpreter::init();
}

PipeSample::~PipeSample() {
    PerfStat::destroy();
}

std::string PipeSample::getTuringHome() const {
    const char* envStr = std::getenv("TURING_HOME");
    if (!envStr) {
        panic("Environment variable TURING_HOME not found");
    }

    return envStr;
}

bool PipeSample::loadJsonDB(const std::string& jsonDir) {
    spdlog::info("== Loading DB ==");
    spdlog::info("Path: {}", jsonDir);

    auto t0 = Clock::now();

    const bool res = Neo4jImporter::importJsonDir(
        *_jobSystem,
        _system->getDefaultDB(),
        db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
        db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
        Neo4jImporter::ImportJsonDirArgs {
            ._jsonDir = jsonDir,
        }
    );

    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");
    if (!res) {
        spdlog::error("Could not load db");
    }

    return res;
}

bool PipeSample::executeQuery(const std::string& queryStr) {
    InterpreterContext interpCtxt(_system);
    MemoryManagerStorage memStorage(1);
    memStorage.initialize();

    EncodingParams encodingParams(EncodingParams::EncodingType::DEBUG_DUMP, std::cout);

    QueryInterpreter interp(&interpCtxt);
    interp.setEncodingParams(&encodingParams);

    const QueryParams queryParams(queryStr, _system->getDefaultDB()->getName());
    MemoryManagerHandle mem = memStorage.alloc();
    const auto res = interp.execute(mem.get(), queryParams);

    if (!res.isOk()) {
        spdlog::error("QueryInterpreter status={}",
                      QueryStatusDescription::value(res.getStatus()));
        return false;
    }

    return true;
}

void PipeSample::createSimpleGraph() {
    DB* db = _system->getDefaultDB();
    auto builder = db->newPartWriter();
    auto* metadata = db->getMetadata();
    auto& labels = metadata->labels();
    auto& labelsets = metadata->labelsets();
    auto& proptypes = metadata->propTypes();
    auto& edgetypes = metadata->edgeTypes();

    const auto getLabelSet = [&](std::initializer_list<std::string> labelList) {
        LabelSet lset;
        for (const auto& l : labelList) {
            lset.set(labels.getOrCreate(l));
        }
        return labelsets.getOrCreate(lset);
    };

    // EdgeTypes
    const EdgeTypeID knowsWellID = edgetypes.getOrCreate("KNOWS_WELL");
    const EdgeTypeID interestedInID = edgetypes.getOrCreate("INTERESTED_IN");

    // Persons
    const EntityID remy = builder->addNode(getLabelSet({"Person", "SoftwareEngineering", "Founder"}));
    const EntityID adam = builder->addNode(getLabelSet({"Person", "Bioinformatics", "Founder"}));
    const EntityID luc = builder->addNode(getLabelSet({"Person", "SoftwareEngineering"}));
    const EntityID maxime = builder->addNode(getLabelSet({"Person", "Bioinformatics"}));
    const EntityID martina = builder->addNode(getLabelSet({"Person", "Bioinformatics"}));

    // Interests
    const EntityID ghosts = builder->addNode(getLabelSet({"Interest"}));
    const EntityID bio = builder->addNode(getLabelSet({"Interest"}));
    const EntityID cooking = builder->addNode(getLabelSet({"Interest"}));
    const EntityID paddle = builder->addNode(getLabelSet({"Interest"}));
    const EntityID animals = builder->addNode(getLabelSet({"Interest"}));
    const EntityID computers = builder->addNode(getLabelSet({"Interest"}));
    const EntityID eighties = builder->addNode(getLabelSet({"Interest"}));

    // Property types
    const PropertyType name = proptypes.getOrCreate("name", types::String::_valueType);

    // Edges
    const EdgeRecord e01 = builder->addEdge(knowsWellID, remy, adam);
    const EdgeRecord e02 = builder->addEdge(knowsWellID, adam, remy);
    const EdgeRecord e03 = builder->addEdge(interestedInID, remy, ghosts);
    const EdgeRecord e04 = builder->addEdge(interestedInID, remy, computers);
    const EdgeRecord e05 = builder->addEdge(interestedInID, remy, eighties);
    const EdgeRecord e06 = builder->addEdge(interestedInID, adam, bio);
    const EdgeRecord e07 = builder->addEdge(interestedInID, adam, cooking);
    const EdgeRecord e08 = builder->addEdge(interestedInID, luc, animals);
    const EdgeRecord e09 = builder->addEdge(interestedInID, luc, computers);
    const EdgeRecord e10 = builder->addEdge(interestedInID, maxime, bio);
    const EdgeRecord e11 = builder->addEdge(interestedInID, maxime, paddle);
    const EdgeRecord e12 = builder->addEdge(interestedInID, martina, cooking);

    // Node Properties
    builder->addNodeProperty<types::String>(remy, name._id, "Remy");
    builder->addNodeProperty<types::String>(adam, name._id, "Adam");
    builder->addNodeProperty<types::String>(luc, name._id, "Luc");
    builder->addNodeProperty<types::String>(maxime, name._id, "Maxime");
    builder->addNodeProperty<types::String>(martina, name._id, "Martina");
    builder->addNodeProperty<types::String>(ghosts, name._id, "Ghosts");
    builder->addNodeProperty<types::String>(bio, name._id, "Bio");
    builder->addNodeProperty<types::String>(cooking, name._id, "Cooking");
    builder->addNodeProperty<types::String>(paddle, name._id, "Paddle");
    builder->addNodeProperty<types::String>(animals, name._id, "Animals");
    builder->addNodeProperty<types::String>(computers, name._id, "Computers");
    builder->addNodeProperty<types::String>(eighties, name._id, "Eighties");

    // Edge Properties
    builder->addEdgeProperty<types::String>(e01, name._id, "Remy -> Adam");
    builder->addEdgeProperty<types::String>(e02, name._id, "Adam -> Remy");
    builder->addEdgeProperty<types::String>(e03, name._id, "Remy -> Ghosts");
    builder->addEdgeProperty<types::String>(e04, name._id, "Remy -> Computers");
    builder->addEdgeProperty<types::String>(e05, name._id, "Remy -> Eighties");
    builder->addEdgeProperty<types::String>(e06, name._id, "Adam -> Bio");
    builder->addEdgeProperty<types::String>(e07, name._id, "Adam -> Cooking");
    builder->addEdgeProperty<types::String>(e08, name._id, "Luc -> Animals");
    builder->addEdgeProperty<types::String>(e09, name._id, "Luc -> Computers");
    builder->addEdgeProperty<types::String>(e10, name._id, "Maxime -> Bio");
    builder->addEdgeProperty<types::String>(e11, name._id, "Maxime -> Paddle");
    builder->addEdgeProperty<types::String>(e12, name._id, "Martina -> Cooking");

    builder->commit(*_jobSystem);
}

void PipeSample::startHttpServer() {
    _server->start();
}
