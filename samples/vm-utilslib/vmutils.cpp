#include "vmutils.h"

#include <range/v3/view/zip.hpp>

#include "DB.h"
#include "DBAccess.h"
#include "DataBuffer.h"
#include "LogSetup.h"
#include "LogUtils.h"
#include "Neo4j/ParserConfig.h"
#include "Neo4jImporter.h"
#include "Panic.h"
#include "PerfStat.h"
#include "Time.h"

VMSample VMSample::createSample(const std::string& sampleName) {
    LogSetup::setupLogFileBacked(sampleName + ".log");
    PerfStat::init(sampleName + ".perf");
    spdlog::set_level(spdlog::level::info);
    const std::string turingHome = std::getenv("TURING_HOME");

    auto jobSystem = std::make_unique<JobSystem>();
    auto system = std::make_unique<db::SystemManager>();
    auto assembler = std::make_unique<db::Assembler>();
    auto vm = std::make_unique<db::VM>(system.get());
    auto program = std::make_unique<db::Program>();

    jobSystem->initialize();

    spdlog::info("== Init VM ==");
    auto t0 = Clock::now();
    vm->initialize();
    logt::ElapsedTime(Microseconds(Clock::now() - t0).count(), "us");

    return VMSample {
        ._turingHome = turingHome,
        ._sampleDir = turingHome + "/samples/" + sampleName,
        ._jobSystem = std::move(jobSystem),
        ._system = std::move(system),
        ._assembler = std::move(assembler),
        ._vm = std::move(vm),
        ._program = std::move(program),
    };
}

void VMSample::destroy() {
    PerfStat::destroy();
}

bool VMSample::loadJsonDB(std::string_view jsonDir) const {
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
        });

    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");
    if (!res) {
        spdlog::error("Could not load db");
    }

    return res;
}

void VMSample::createSimpleGraph() const {
    using namespace db;

    auto* db = _system->getDefaultDB();
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

bool VMSample::generateFromFile(std::string_view programPath) const {
    spdlog::info("== Code generation ==");
    spdlog::info("Path: {}", programPath);
    auto t0 = Clock::now();

    _program->clear();
    const bool res = _assembler->generateFromFile(*_program, programPath);

    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");
    if (!res) {
        spdlog::error("Could not generate byte code");
    }

    return res;
}

bool VMSample::generateFromString(const std::string& programString) const {
    spdlog::info("== Code generation ==");
    auto t0 = Clock::now();

    _program->clear();
    const bool res = _assembler->generate(*_program, programString);

    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");
    if (!res) {
        spdlog::error("Could not generate byte code");
    }

    return res;
}

void VMSample::execute() const {
    spdlog::info("== Execution ==");
    auto t0 = Clock::now();

    _vm->exec(_program.get());
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");
}

#define PRINT_CASE(TType)                                               \
    case TType::staticKind(): {                                         \
        const auto& c = *static_cast<const TType*>(col);                \
        lineCount = std::min(c.size(), maxLineCount);                   \
        if (lines.empty()) {                                            \
            for (size_t i = 0; i < lineCount; i++) {                    \
                lines.emplace_back();                                   \
            }                                                           \
            totalLineCount = c.size();                                  \
        } else {                                                        \
            msgbioassert(totalLineCount == c.size(),                    \
                         fmt::format("Output is ill formed: {} vs. {}", \
                                     totalLineCount,                    \
                                     c.size())                          \
                             .c_str());                                 \
        }                                                               \
        for (size_t i = 0; i < lineCount; i++) {                        \
            lines[i] += fmt::format("{1:^{0}}", colSize, c[i]);         \
        }                                                               \
        break;                                                          \
    }

void VMSample::printOutput(std::initializer_list<std::string_view> colNames,
                           uint8_t outRegister,
                           size_t maxLineCount,
                           size_t colSize) const {
    using namespace db;

    const auto& out = getOutput(outRegister);

    if (out.empty()) {
        spdlog::error("Output is empty");
        return;
    }

    std::string header;
    for (const auto& name : colNames) {
        header += fmt::format("{1:^{0}}", colSize, name);
    }

    spdlog::info(header);
    spdlog::info(std::string(colSize * colNames.size(), '-'));

    const auto& cols = out.columns();
    std::vector<std::string> lines;
    size_t lineCount = 0;
    size_t totalLineCount = 0;
    for (const auto* col : cols) {
        switch (col->getKind()) {
            PRINT_CASE(ColumnVector<EntityID>)
            PRINT_CASE(ColumnVector<types::UInt64::Primitive>)
            PRINT_CASE(ColumnVector<types::Int64::Primitive>)
            PRINT_CASE(ColumnVector<types::Double::Primitive>)
            PRINT_CASE(ColumnVector<types::String::Primitive>)
            PRINT_CASE(ColumnVector<types::Bool::Primitive>)
            default: {
                panic("Printing column of type {} is not supported", col->getKind());
            }
        }
    }

    for (const auto& line : lines) {
        spdlog::info(line);
    }
    spdlog::info(std::string(colSize * colNames.size(), '-'));
    spdlog::info("NLines in output: {}", totalLineCount);
}

db::DBAccess VMSample::readDB() const {
    return _system->getDefaultDB()->access();
}

db::EntityID VMSample::findNode(const std::string& ptName, const std::string& prop) const {
    spdlog::info("== Finding node ==");
    spdlog::info("Prop: [{}]: {}", ptName, prop);
    auto t0 = Clock::now();

    auto* db = _system->getDefaultDB();
    const auto access = db->access();
    const auto& propTypes = db->getMetadata()->propTypes();
    db::PropertyType pt = propTypes.get(ptName);

    if (!pt._id.isValid()) {
        spdlog::error("Property type {} does not exist", ptName);
        return {};
    }

    const auto nhs_nos = access.scanNodeProperties<db::types::String>(pt._id);
    auto it = nhs_nos.begin();
    for (; it.isValid(); ++it) {
        const auto& v = it.get();
        if (v == prop) {
            logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");
            return it.getCurrentNodeID();
        }
    }

    spdlog::error("Could not find node with prop {} of propType {}",
                  prop, ptName);
    return {};
}

const db::Block& VMSample::getOutput(uint8_t reg) const {
    return _vm->readRegister<db::OutputWriter>(reg)->getResult();
}
