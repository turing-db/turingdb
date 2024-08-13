#pragma once

#include <spdlog/spdlog.h>
#include <string>

#include "Assembler.h"
#include "JobSystem.h"
#include "Program.h"
#include "SystemManager.h"
#include "VM.h"
#include "DBAccess.h"

class VMSample {
public:
    std::string _turingHome;
    std::string _sampleDir;
    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<db::SystemManager> _system;
    std::unique_ptr<db::Assembler> _assembler;
    std::unique_ptr<db::VM> _vm;
    std::unique_ptr<db::Program> _program;

    static VMSample createSample(const std::string& sampleName);

    void destroy();
    bool loadJsonDB(std::string_view jsonDir) const;
    void createSimpleGraph() const;
    bool generateFromFile(std::string_view programPath) const;
    bool generateFromString(const std::string& programString) const;
    void execute() const;
    bool executeFile(std::string_view programPath) const;
    bool executeString(const std::string& programString) const;
    void printOutput(std::initializer_list<std::string_view> colNames,
                     uint8_t outRegister = 0,
                     size_t maxLineCount = 20,
                     size_t colSize = 12) const;
    db::DBAccess readDB() const;

    db::EntityID findNode(const std::string& ptName, const std::string& prop) const;

    const db::Block& getOutput(uint8_t reg = 0) const;
};
