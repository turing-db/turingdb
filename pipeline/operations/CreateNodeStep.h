#pragma once

#include "ExecutionContext.h"

namespace db {

class EntityPattern;
class DataPartBuilder;

class CreateNodeStep {
public:
    struct Tag {};

    explicit CreateNodeStep(const EntityPattern* data);
    CreateNodeStep(CreateNodeStep&& other) = default;
    ~CreateNodeStep();

    void prepare(ExecutionContext* ctxt);

    void reset() {
    }

    bool isFinished() const {
        return true;
    }

    void execute();

    void describe(std::string& descr) const;

private:
    DataPartBuilder* _builder {nullptr};
    const EntityPattern* _data {nullptr};
};

}
