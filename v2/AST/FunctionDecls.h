#pragma once

#include <string_view>
#include <memory>
#include <map>

#include "FunctionSignature.h"

namespace db::v2 {

class FunctionDecls {
public:
    class FunctionSignatureBuilder {
    public:
        explicit FunctionSignatureBuilder(FunctionSignature* signature)
            : _signature(signature)
        {
        }

        FunctionSignatureBuilder& setIsAggregate(bool isAggregate) {
            _signature->_isAggregate = isAggregate;
            return *this;
        }

        FunctionSignatureBuilder& setIsDatabaseProcedure(bool isDatabaseProcedure) {
            _signature->_isDatabaseProcedure = isDatabaseProcedure;
            return *this;
        }

        FunctionSignatureBuilder& setArguments(std::initializer_list<EvaluatedType> types) {
            _signature->_argumentTypes = types;
            return *this;
        }

        FunctionSignatureBuilder& setReturnTypes(std::initializer_list<FunctionReturnType> types) {
            _signature->_returnTypes = types;
            return *this;
        }

        FunctionSignatureBuilder& addReturnType(EvaluatedType type, std::string_view name) {
            _signature->_returnTypes.emplace_back(type, name);
            return *this;
        }

    private:
        FunctionSignature* _signature {nullptr};
    };

    FunctionDecls();
    ~FunctionDecls();

    FunctionDecls(const FunctionDecls&) = delete;
    FunctionDecls(FunctionDecls&&) = delete;
    FunctionDecls& operator=(const FunctionDecls&) = delete;
    FunctionDecls& operator=(FunctionDecls&&) = delete;

    static std::unique_ptr<FunctionDecls> createDefault();

    FunctionSignatureBuilder create(std::string_view fullName);

    auto get(std::string_view fullName) const {
        return _decls.equal_range(fullName);
    }

private:
    std::multimap<std::string_view, std::unique_ptr<FunctionSignature>> _decls;
};

}
