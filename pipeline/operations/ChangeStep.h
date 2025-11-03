#pragma once

#include <string>
#include <variant>

#include "ExecutionContext.h"
#include "columns/ColumnVector.h"
#include "versioning/ChangeID.h"
#include "views/GraphView.h"
#include "ChangeOpType.h"

namespace db {

class ExecutionContext;
class SystemManager;
using ChangeInfo = std::variant<ChangeID, std::string>;

class ChangeStep {
public:
    struct Tag {};

    ChangeStep(ChangeOpType type,
               ColumnVector<ChangeID>* output);
    ~ChangeStep();

    void prepare(ExecutionContext* ctxt);

    bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};
    Graph* _graph {nullptr};
    GraphView _view;
    ChangeOpType _type {};
    ChangeInfo _changeInfo;
    ColumnVector<ChangeID>* _output {nullptr};
    Transaction* _tx {nullptr};

    void createChange() const;
    void submitChange() const;
    void deleteChange() const;
    void listChanges() const;
};

}
