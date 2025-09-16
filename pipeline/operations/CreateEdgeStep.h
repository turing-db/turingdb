#pragma once

#include <span>

#include "ExecutionContext.h"
#include "columns/ColumnIDs.h"

namespace db {

class EntityPattern;
class DataPartBuilder;

class CreateEdgeStep {
public:
    struct Tag {};

    explicit CreateEdgeStep(const EntityPattern* src,
                            const EntityPattern* edge,
                            const EntityPattern* tgt);
    CreateEdgeStep(CreateEdgeStep&& other) = default;
    ~CreateEdgeStep();

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
    const EntityPattern* _src {nullptr};
    const EntityPattern* _edge {nullptr};
    const EntityPattern* _tgt {nullptr};
};

}
