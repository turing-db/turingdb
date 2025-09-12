#pragma once

#include "ExecutionContext.h"

namespace db {

class EntityPattern;
class DataPartBuilder;
class CommitWriteBuffer;

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

    static void createNode(DataPartBuilder* builder, const EntityPattern* data,
                           CommitWriteBuffer* writeBuffer);

private:
    DataPartBuilder* _builder {nullptr};
    CommitWriteBuffer* _writeBuffer {nullptr};
    const EntityPattern* _data {nullptr};
};

}
