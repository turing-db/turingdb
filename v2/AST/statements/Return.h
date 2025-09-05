#pragma once

#include <memory>

#include "Statement.h"

namespace db::v2 {

class Projection;

class Return : public Statement {
public:
    Return() = default;
    ~Return() override = default;

    Return(const Return&) = delete;
    Return& operator=(const Return&) = delete;
    Return(Return&&) = delete;
    Return& operator=(Return&&) = delete;

    Return(Projection* projection)
        : _projection(projection)
    {
    }

    static std::unique_ptr<Return> create(Projection* projection) {
        return std::make_unique<Return>(projection);
    }

    const Projection& getProjection() const {
        return *_projection;
    }

private:
    Projection* _projection {nullptr};
};

}
