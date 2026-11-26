#pragma once

#include <stddef.h>
#include <optional>
#include <string_view>
#include <vector>

#include "NLStepBuffers.h"
#include "columns/Column.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"

namespace db {

class NLExecutionContext;

using NLStringArgumentRead = std::optional<types::String::Primitive> (*)(const Column* input,
                                                                        size_t row,
                                                                        std::string_view functionName);

using NLIntegerArgumentRead = std::optional<types::Int64::Primitive> (*)(const Column* input,
                                                                        size_t row,
                                                                        std::string_view functionName);

using NLPointArgumentRead = std::optional<types::Point::Primitive> (*)(const Column* input,
                                                                      size_t row,
                                                                      std::string_view functionName);

// The arguments of point.distance or point.withinBBox, each a column read one point per row
class NLPointFunctionData : public NLFunctionData {
public:
    struct PointArgument {
        const Column* _column {nullptr};
        NLPointArgumentRead _read {nullptr};
    };

    explicit NLPointFunctionData(Column* result)
        : _result(result)
    {
    }

    Column* getResult() const { return _result; }

    const std::vector<PointArgument>& getArguments() const { return _arguments; }

    void addArgument(const PointArgument& argument) { _arguments.push_back(argument); }

private:
    Column* _result {nullptr};
    std::vector<PointArgument> _arguments;
};

class NLSubstringData : public NLFunctionData {
public:
    struct StringArgument {
        const Column* _column {nullptr};
        NLStringArgumentRead _read {nullptr};
    };

    struct IntegerArgument {
        const Column* _column {nullptr};
        NLIntegerArgumentRead _read {nullptr};
    };

    NLSubstringData(Column* result,
                    const StringArgument& string,
                    const IntegerArgument& start,
                    const IntegerArgument& length)
        : _result(result),
        _string(string),
        _start(start),
        _length(length)
    {
    }

    Column* getResult() const { return _result; }

    const StringArgument& getString() const { return _string; }
    const IntegerArgument& getStart() const { return _start; }
    const IntegerArgument& getLength() const { return _length; }

private:
    Column* _result {nullptr};
    StringArgument _string;
    IntegerArgument _start;
    IntegerArgument _length;
};

using NLStringListArgumentRead = bool (*)(const Column* input,
                                          size_t row,
                                          std::string_view functionName,
                                          std::vector<types::String::Primitive>& strings);

class NLSplitData : public NLFunctionData {
public:
    using StringArgument = NLSubstringData::StringArgument;

    struct DelimitersArgument {
        const Column* _column {nullptr};
        NLStringListArgumentRead _read {nullptr};
    };

    NLSplitData(Column* result,
                NLStepBuffers* stepBuffers,
                const StringArgument& string,
                const DelimitersArgument& delimiters)
        : _result(result),
        _stepBuffers(stepBuffers),
        _string(string),
        _delimiters(delimiters)
    {
    }

    Column* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    const StringArgument& getString() const { return _string; }
    const DelimitersArgument& getDelimiters() const { return _delimiters; }

private:
    Column* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    StringArgument _string;
    DelimitersArgument _delimiters;
};

class NLStringEndData : public NLFunctionData {
public:
    using StringArgument = NLSubstringData::StringArgument;
    using IntegerArgument = NLSubstringData::IntegerArgument;

    NLStringEndData(Column* result,
                    const StringArgument& string,
                    const IntegerArgument& length)
        : _result(result),
        _string(string),
        _length(length)
    {
    }

    Column* getResult() const { return _result; }

    const StringArgument& getString() const { return _string; }
    const IntegerArgument& getLength() const { return _length; }

private:
    Column* _result {nullptr};
    StringArgument _string;
    IntegerArgument _length;
};

class NLReplaceData : public NLFunctionData {
public:
    using StringArgument = NLSubstringData::StringArgument;

    NLReplaceData(Column* result,
                  NLStepBuffers* stepBuffers,
                  const StringArgument& string,
                  const StringArgument& search,
                  const StringArgument& replacement)
        : _result(result),
        _stepBuffers(stepBuffers),
        _string(string),
        _search(search),
        _replacement(replacement)
    {
    }

    Column* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    const StringArgument& getString() const { return _string; }
    const StringArgument& getSearch() const { return _search; }
    const StringArgument& getReplacement() const { return _replacement; }

private:
    Column* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    StringArgument _string;
    StringArgument _search;
    StringArgument _replacement;
};

using NLUnaryFunctionKernel = void (*)(NLExecutionContext* context,
                                       Column* result,
                                       const Column* input,
                                       NLStepBuffers* stepBuffers);

class NLUnaryFunctionData : public NLFunctionData {
public:
    NLUnaryFunctionData(const Column* input,
                        Column* result,
                        NLUnaryFunctionKernel kernel,
                        NLStepBuffers* stepBuffers)
        : _input(input),
        _result(result),
        _kernel(kernel),
        _stepBuffers(stepBuffers)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getResult() const { return _result; }
    NLUnaryFunctionKernel getKernel() const { return _kernel; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

private:
    const Column* _input {nullptr};
    Column* _result {nullptr};
    NLUnaryFunctionKernel _kernel {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
};

}
