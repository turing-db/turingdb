#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "StringRowSink.h"
#include "TuringTest.h"

namespace db {
class Graph;
class JobSystem;
class ParameterMap;
}

namespace turing::test {

// Runs a db program parsed from text on the SimpleGraph fixture, with the parameter map
// the translator binds the program's parameters from
class ParameterBindingTest : public TuringTest {
public:
    ParameterBindingTest();
    ~ParameterBindingTest() override;

protected:
    void initialize() override;
    void terminate() override;

    void execute(const char* programText, const db::ParameterMap* parameters, StringRowSink& sink);
    void expectRows(const char* programText, const db::ParameterMap& parameters, const std::vector<StringRowSink::Row>& expected);
    void expectRowCount(const char* programText, const db::ParameterMap& parameters, size_t expected);
    void expectError(const char* programText, const db::ParameterMap* parameters, std::string_view reason);

private:
    std::unique_ptr<db::JobSystem> _jobSystem;
    std::unique_ptr<db::Graph> _graph;
};

}
