#pragma once

#include <stdint.h>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace db {

// One dump an EXPLAIN prefix can ask for. A single optimisation pass is named through
// requestPass instead; PASSES stands for the pipeline as a whole, and asks for CODEGEN
// with it since a dump per pass is read against the module the first pass saw.
enum class ExplainStage {
    AST = 0,
    VDG,
    CODEGEN,
    PASSES,
    DB,
    NL,
};

// What the EXPLAIN prefix of a query asks the engine to report, as the parser read
// it. Stage words and the before / after / around pass selectors are resolved here;
// a pass name is only checked against the pipeline that runs it.
class ExplainRequest {
public:
    ExplainRequest();
    ~ExplainRequest();

    // The db program and the nl program it lowers to: what a bare EXPLAIN reports
    void requestDefaults();
    void requestAll();

    // False when the word names no stage
    bool requestStage(std::string_view word);

    // False when the word is not one of before, after and around
    bool requestPass(std::string_view selector, std::string_view passName);

    bool isRequested(ExplainStage stage) const;
    bool isPassPrintedBefore(std::string_view passName) const;
    bool isPassPrintedAfter(std::string_view passName) const;
    bool hasNamedPasses() const { return !_passesBefore.empty() || !_passesAfter.empty(); }

    // The first named pass the given pipeline does not run, empty when it runs them all
    std::string_view findUnknownPass(std::span<const std::string_view> pipelinePasses) const;

    static std::string_view getStageName(ExplainStage stage);
    static void describeOptions(std::string_view word, std::string& message);

private:
    uint32_t _stages {0};
    std::vector<std::string_view> _passesBefore;
    std::vector<std::string_view> _passesAfter;

    void request(ExplainStage stage);
};

}
