#include "PipeUtils.h"

#include <iostream>

#include <stdlib.h>
#include <spdlog/spdlog.h>

#include "SystemManager.h"
#include "JobSystem.h"
#include "Neo4j/ParserConfig.h"
#include "Neo4jImporter.h"

#include "DB.h"
#include "QueryInterpreter.h"
#include "InterpreterContext.h"
#include "QueryParams.h"
#include "QueryPlannerParams.h"
#include "DBAccess.h"
#include "DataBuffer.h"

#include "DBServerConfig.h"
#include "Server.h"
#include "DBServerContext.h"
#include "DBServerSession.h"

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
    _system = std::make_unique<db::SystemManager>();
    _jobSystem = std::make_unique<JobSystem>();
    _jobSystem->initialize();
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

    const bool res = db::Neo4jImporter::importJsonDir(
        *_jobSystem,
        _system->getDefaultDB(),
        db::nodeCountLimit,
        db::edgeCountLimit,
        db::Neo4jImporter::ImportJsonDirArgs {
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
    InterpreterContext interpCtxt(_system.get());

    EncodingParams encodingParams(EncodingParams::EncodingType::DEBUG_DUMP, std::cout);

    QueryInterpreter interp(&interpCtxt);
    interp.setEncodingParams(&encodingParams);

    const QueryParams queryParams(queryStr, _system->getDefaultDB()->getName());
    const auto res = interp.execute(queryParams);

    if (res != QueryStatus::OK) {
        spdlog::error("QueryInterpreter status={}", (size_t)res);
        return false;
    }
    
    return true;
}

void PipeSample::createSimpleGraph() {
    DB* db = _system->getDefaultDB();
    auto buf = db->uniqueAccess().newDataBuffer();
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
    const EntityID remy = buf->addNode(getLabelSet({"Person", "SoftwareEngineering", "Founder"}));
    const EntityID adam = buf->addNode(getLabelSet({"Person", "Bioinformatics", "Founder"}));
    const EntityID luc = buf->addNode(getLabelSet({"Person", "SoftwareEngineering"}));
    const EntityID maxime = buf->addNode(getLabelSet({"Person", "Bioinformatics"}));
    const EntityID martina = buf->addNode(getLabelSet({"Person", "Bioinformatics"}));

    // Interests
    const EntityID ghosts = buf->addNode(getLabelSet({"Interest"}));
    const EntityID bio = buf->addNode(getLabelSet({"Interest"}));
    const EntityID cooking = buf->addNode(getLabelSet({"Interest"}));
    const EntityID paddle = buf->addNode(getLabelSet({"Interest"}));
    const EntityID animals = buf->addNode(getLabelSet({"Interest"}));
    const EntityID computers = buf->addNode(getLabelSet({"Interest"}));
    const EntityID eighties = buf->addNode(getLabelSet({"Interest"}));

    // Property types
    const PropertyType name = proptypes.getOrCreate("name", types::String::_valueType);

    // Edges
    const EdgeRecord e01 = buf->addEdge(knowsWellID, remy, adam);
    const EdgeRecord e02 = buf->addEdge(knowsWellID, adam, remy);
    const EdgeRecord e03 = buf->addEdge(interestedInID, remy, ghosts);
    const EdgeRecord e04 = buf->addEdge(interestedInID, remy, computers);
    const EdgeRecord e05 = buf->addEdge(interestedInID, remy, eighties);
    const EdgeRecord e06 = buf->addEdge(interestedInID, adam, bio);
    const EdgeRecord e07 = buf->addEdge(interestedInID, adam, cooking);
    const EdgeRecord e08 = buf->addEdge(interestedInID, luc, animals);
    const EdgeRecord e09 = buf->addEdge(interestedInID, luc, computers);
    const EdgeRecord e10 = buf->addEdge(interestedInID, maxime, bio);
    const EdgeRecord e11 = buf->addEdge(interestedInID, maxime, paddle);
    const EdgeRecord e12 = buf->addEdge(interestedInID, martina, cooking);

    // Node Properties
    buf->addNodeProperty<types::String>(remy, name._id, "Remy");
    buf->addNodeProperty<types::String>(adam, name._id, "Adam");
    buf->addNodeProperty<types::String>(luc, name._id, "Luc");
    buf->addNodeProperty<types::String>(maxime, name._id, "Maxime");
    buf->addNodeProperty<types::String>(martina, name._id, "Martina");
    buf->addNodeProperty<types::String>(ghosts, name._id, "Ghosts");
    buf->addNodeProperty<types::String>(bio, name._id, "Bio");
    buf->addNodeProperty<types::String>(cooking, name._id, "Cooking");
    buf->addNodeProperty<types::String>(paddle, name._id, "Paddle");
    buf->addNodeProperty<types::String>(animals, name._id, "Animals");
    buf->addNodeProperty<types::String>(computers, name._id, "Computers");
    buf->addNodeProperty<types::String>(eighties, name._id, "Eighties");

    // Edge Properties
    buf->addEdgeProperty<types::String>(e01, name._id, "Remy -> Adam");
    buf->addEdgeProperty<types::String>(e02, name._id, "Adam -> Remy");
    buf->addEdgeProperty<types::String>(e03, name._id, "Remy -> Ghosts");
    buf->addEdgeProperty<types::String>(e04, name._id, "Remy -> Computers");
    buf->addEdgeProperty<types::String>(e05, name._id, "Remy -> Eighties");
    buf->addEdgeProperty<types::String>(e06, name._id, "Adam -> Bio");
    buf->addEdgeProperty<types::String>(e07, name._id, "Adam -> Cooking");
    buf->addEdgeProperty<types::String>(e08, name._id, "Luc -> Animals");
    buf->addEdgeProperty<types::String>(e09, name._id, "Luc -> Computers");
    buf->addEdgeProperty<types::String>(e10, name._id, "Maxime -> Bio");
    buf->addEdgeProperty<types::String>(e11, name._id, "Maxime -> Paddle");
    buf->addEdgeProperty<types::String>(e12, name._id, "Martina -> Cooking");

    auto part = db->uniqueAccess().createDataPart(std::move(buf));
    part->load(db->access(), *_jobSystem);
    db->uniqueAccess().pushDataPart(std::move(part));
}

void PipeSample::startHttpServer() {
    DBServerConfig config;
    InterpreterContext interpCtxt(_system.get());
    DBServerContext serverContext(&interpCtxt);
    Server<DBServerContext, DBServerSession> server(&serverContext,
                                                    config.getServerConfig());
    server.start();
}