#pragma once

#include "PlanGraph.h"

#include "TuringException.h"

namespace db {

class PlanGraphTester {
public:
    explicit PlanGraphTester(const PlanGraphNode* root)
        : _current(root)
    {
    }

    PlanGraphTester& expect(PlanGraphOpcode opcode) {
        const auto currentOpcode = _current->getOpcode();
        if (currentOpcode != opcode) {
            const auto currentOpStr = std::string(PlanGraphOpcodeDescription::value(currentOpcode));
            const auto expectedOpStr = std::string(PlanGraphOpcodeDescription::value(opcode));
            throw TuringException(std::string("Wrong opcode for PlanGraphNode, got=")
                                  + currentOpStr
                                  + " expected="
                                  + expectedOpStr);
        }

        const auto& outs = _current->outputs();
        if (!outs.empty()) {
            _current = outs.front();
        }

        return *this;
    }

private:
    const PlanGraphNode* _current {nullptr};
};

}
