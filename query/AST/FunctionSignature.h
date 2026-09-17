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

    // Whether the list this returns holds the values of its own argument, so a caller
    // reading an element back knows the type it has - collect, and nothing else today.
    bool collectsItsArgument() const { return _collectsItsArgument; }

    // Whether this takes any number of values of one type and answers the type they
    // share - coalesce, and nothing else today. Such a signature declares no argument of
    // its own: the analyzer unifies the arguments against each other, as it does the
    // branches of a CASE, and the type they share is what the call returns.
    bool unifiesItsArguments() const { return _unifiesItsArguments; }

    // Whether the list this returns nests as deeply over the same elements as its own
    // argument, so an UNWIND of it binds what an UNWIND of the argument would - tail.
    bool returnsItsArgumentShape() const { return _returnsItsArgumentShape; }

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

    void setCollectsItsArgument(bool collects) { _collectsItsArgument = collects; }

    void setUnifiesItsArguments(bool unifies) { _unifiesItsArguments = unifies; }

    void setReturnsItsArgumentShape(bool returnsShape) { _returnsItsArgumentShape = returnsShape; }

private:
    std::string_view _fullName;
    ArgumentTypes _argumentTypes;
    std::vector<FunctionReturnType> _returnTypes;
    size_t _requiredArgCount {0};
    bool _isAggregate {false};
    bool _isProcedure {false};
    bool _collectsItsArgument {false};
    bool _unifiesItsArguments {false};
    bool _returnsItsArgumentShape {false};
};

}

