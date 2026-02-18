#pragma once

#include <vector>
#include <string_view>

#include "decl/EvaluatedType.h"

namespace db {

class FunctionReturnType {
public:
    FunctionReturnType(EvaluatedType type)
        : _type(type)
    {
    }

    EvaluatedType getType() const { return _type; }

    std::string_view getName() const { return _name; }

    void setName(std::string_view name) { _name = name; }

private:
    EvaluatedType _type {EvaluatedType::Invalid};
    std::string_view _name;
};

class FunctionSignature {
public:
    using ArgumentTypes = std::vector<EvaluatedType>;
    using ReturnTypes = std::vector<FunctionReturnType>;

    explicit FunctionSignature(std::string_view fullName);
    ~FunctionSignature();

    std::string_view getFullName() const { return _fullName; }

    const ArgumentTypes& argumentTypes() const { return _argumentTypes; }

    const ReturnTypes& returnTypes() const { return _returnTypes; }

    bool isAggregate() const { return _isAggregate; }

    bool isProcedure() const { return _isProcedure; }

    void setArguments(std::vector<EvaluatedType>&& args) {
        _argumentTypes = std::move(args);
    }

    void setReturnTypes(std::vector<FunctionReturnType>&& ret) {
        _returnTypes = std::move(ret);
    }

    void setIsAggregate(bool aggregate) { _isAggregate = aggregate; }

    void setIsProcedure(bool procedure) { _isProcedure = procedure; }

private:
    std::string_view _fullName;
    std::vector<EvaluatedType> _argumentTypes;
    std::vector<FunctionReturnType> _returnTypes;
    bool _isAggregate {false};
    bool _isProcedure {false};
};

}

