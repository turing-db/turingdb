#pragma once

#include <string_view>
#include <vector>

namespace db {

enum class ProcedureReturnType : uint8_t {
    INVALID = 0,
    NODE,
    EDGE,
    LABEL_ID,
    EDGE_TYPE_ID,
    PROPERTY_TYPE_ID,
    VALUE_TYPE,
    UINT_64,
    INT64,
    DOUBLE,
    BOOL,
    STRING_VIEW,
    STRING,
};

struct ProcedureReturnValue {
    std::string_view _name;
    ProcedureReturnType _type {};
};

class ProcedureReturnValues {
public:
    using Vector = std::vector<ProcedureReturnValue>;

    ProcedureReturnValues() = default;
    ~ProcedureReturnValues() = default;

    ProcedureReturnValues(std::initializer_list<ProcedureReturnValue> values)
        : _returnValues(values)
    {
    }

    ProcedureReturnValues(const ProcedureReturnValues&) = default;
    ProcedureReturnValues(ProcedureReturnValues&&) noexcept = default;
    ProcedureReturnValues& operator=(const ProcedureReturnValues&) = default;
    ProcedureReturnValues& operator=(ProcedureReturnValues&&) noexcept = default;

    void add(std::string_view name, ProcedureReturnType type) {
        _returnValues.emplace_back(name, type);
    }

    [[nodiscard]] size_t size() const {
        return _returnValues.size();
    }

    [[nodiscard]] const ProcedureReturnValue& operator[](size_t i) const {
        return _returnValues[i];
    }

    Vector::const_iterator begin() const {
        return _returnValues.begin();
    }

    Vector::const_iterator end() const {
        return _returnValues.end();
    }

private:
    Vector _returnValues;
};

}
