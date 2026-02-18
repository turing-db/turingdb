#pragma once

#include <string_view>

namespace db {

class FunctionSignature;

class FunctionResolver {
public:
    class FunctionSignatureRange {
    public:
        using Iterator = FunctionSignature**;

        FunctionSignatureRange() = default;

        FunctionSignatureRange(Iterator begin, Iterator end)
            : _begin(begin),
            _end(end)
        {
        }

        Iterator begin() const { return _begin; }
        Iterator end() const { return _end; }
        bool empty() const { return _begin == _end; }

    private:
        Iterator _begin {nullptr};
        Iterator _end {nullptr};
    };

    virtual ~FunctionResolver() = default;

    virtual FunctionSignatureRange lookup(std::string_view fullName) = 0;
};

}
