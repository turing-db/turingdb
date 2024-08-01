#include "vmutils.h"

#include <range/v3/view/zip.hpp>

#include "DB.h"
#include "DBAccess.h"
#include "DataBuffer.h"
#include "LogSetup.h"
#include "LogUtils.h"
#include "Neo4j/ParserConfig.h"
#include "Neo4jImporter.h"
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
    buf->addEdge(knowsWellID, remy, adam);
    buf->addEdge(knowsWellID, adam, remy);
    buf->addEdge(interestedInID, remy, ghosts);
    buf->addEdge(interestedInID, remy, computers);
    buf->addEdge(interestedInID, remy, eighties);
    buf->addEdge(interestedInID, adam, bio);
    buf->addEdge(interestedInID, adam, cooking);
    buf->addEdge(interestedInID, luc, animals);
    buf->addEdge(interestedInID, luc, computers);
    buf->addEdge(interestedInID, maxime, bio);
    buf->addEdge(interestedInID, maxime, paddle);
    buf->addEdge(interestedInID, martina, cooking);

    // Properties
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

void VMSample::printOutput(std::initializer_list<std::string_view> colNames,
                           uint8_t outRegister,
                           size_t maxLineCount,
                           size_t colSize) const {

    const auto& out = getOutput(outRegister);

    if (out.empty()) {
        spdlog::error("Output is empty");
        return;
    }

    std::string str;
    for (const auto& name : colNames) {
        str += fmt::format("{1:^{0}}", colSize, name);
    }

    str += "\n";
    for (size_t i = 0; i < out[0].size(); i++) {
        for (size_t j = 0; j < out.size(); j++) {
            if (out[j][i] > 5000000) {
                str += fmt::format("{1:^{0}}", colSize, "...");
            } else {
                str += fmt::format("{1:^{0}}", colSize, out[j][i]);
            }
        }
        str += '\n';
        if (i == maxLineCount) {
            str += "...\n";
            break;
        }
    }
    spdlog::info("\n{}", str);
    spdlog::info("NLines in output: {}", out[0].size());
}

void VMSample::printOutputProperty(const std::string& propName,
                                   std::initializer_list<std::string_view> colNames,
                                   uint8_t outRegister,
                                   size_t maxLineCount,
                                   size_t colSize) const {

    const auto& idOutput = getOutput(outRegister);

    if (idOutput.empty()) { spdlog::error("Output is empty");
        return;
    }

    std::string str;
    for (const auto& name : colNames) {
        str += fmt::format("{1:^{0}}", colSize, name);
    }

    str += "\n";
    auto* db = _system->getDefaultDB();
    const auto* metadata = db->getMetadata();
    const auto ptype = metadata->propTypes().get(propName);
    auto access = db->access();
    db::ColumnVector<db::ColumnVector<std::string>> out;
    out.resize(idOutput.size());
    for (const auto& [ids, col] : ranges::views::zip(idOutput, out)) {
        col.reserve(ids.size());
        for (const auto& v : access.getNodeProperties<db::types::String>(ptype._id, &ids)) {
            col.push_back(v);
        }
    }

    for (size_t i = 0; i < out[0].size(); i++) {
        for (size_t j = 0; j < out.size(); j++) {
            str += fmt::format("{1:^{0}}", colSize, out[j][i]);
        }
        str += '\n';
        if (i == maxLineCount) {
            str += "...\n";
            break;
        }
    }
    spdlog::info("\n{}", str);
    spdlog::info("NLines in output: {}", out[0].size());
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

const db::OutputWriter::Output& VMSample::getOutput(uint8_t reg) const {
    return _vm->readRegister<db::OutputWriter>(reg)->getResult();
}
