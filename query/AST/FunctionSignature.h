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

// A constant argument is read once per call rather than once per row, so only an
// expression that does not vary with the row may be passed to one.
class FunctionArgumentType {
public:
    FunctionArgumentType(EvaluatedType type)
        : _type(type)
    {
    }

    EvaluatedType getType() const { return _type; }

    std::string_view getName() const { return _name; }

    bool isConstant() const { return _constant; }

    void setName(std::string_view name) { _name = name; }

    void setConstant(bool constant) { _constant = constant; }

private:
    EvaluatedType _type {EvaluatedType::Invalid};
    std::string_view _name;
    bool _constant {false};
};

class FunctionSignature {
public:
    using ArgumentTypes = std::vector<FunctionArgumentType>;
    using ReturnTypes = std::vector<FunctionReturnType>;

    explicit FunctionSignature(std::string_view fullName);
    ~FunctionSignature();

    std::string_view getFullName() const { return _fullName; }

    const ArgumentTypes& argumentTypes() const { return _argumentTypes; }

    const ReturnTypes& returnTypes() const { return _returnTypes; }

    bool isAggregate() const { return _isAggregate; }

    bool isProcedure() const { return _isProcedure; }

    bool isV3Only() const { return _isV3Only; }

    // Whether the list this returns holds the values of its own argument, so a caller
    // reading an element back knows the type it has - collect, and nothing else today.
    bool collectsItsArgument() const { return _collectsItsArgument; }

    size_t getMinArgCount() const { return _requiredArgCount; }

    void setArguments(ArgumentTypes&& args) {
        _argumentTypes = std::move(args);
    }

    void setRequiredArgCount(size_t count) { _requiredArgCount = count; }

    void setReturnTypes(std::vector<FunctionReturnType>&& ret) {
        _returnTypes = std::move(ret);
    }

    void setIsAggregate(bool aggregate) { _isAggregate = aggregate; }

    void setIsProcedure(bool procedure) { _isProcedure = procedure; }

    void setIsV3Only(bool v3Only) { _isV3Only = v3Only; }

    void setCollectsItsArgument(bool collects) { _collectsItsArgument = collects; }

private:
    std::string_view _fullName;
    ArgumentTypes _argumentTypes;
    std::vector<FunctionReturnType> _returnTypes;
    size_t _requiredArgCount {0};
    bool _isAggregate {false};
    bool _isProcedure {false};
    bool _collectsItsArgument {false};

    // An overload only the MLIR engine answers: the legacy planner either cannot lay its
    // argument out or reduces it over the wrong rows, so it never matches there
    bool _isV3Only {false};
};

}

