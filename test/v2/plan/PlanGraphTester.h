#pragma once

#include "PlanGraph.h"
#include "decl/VarDecl.h"

#include "TuringException.h"
#include "nodes/VarNode.h"

namespace db::v2 {

class PlanGraphTester {
public:
    explicit PlanGraphTester(const PlanGraphNode* root)
        : _current(root)
    {
    }

    PlanGraphTester& expect(PlanGraphOpcode opcode) {
        if (_current == nullptr) {
            throw TuringException("PlanGraphTester: no more nodes to test");
        }

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
        } else {
            _current = nullptr;
        }

        return *this;
    }

    PlanGraphTester& expectVar(const std::string& var) {
        if (_current == nullptr) {
            throw TuringException("PlanGraphTester: no more nodes to test");
        }

        if (_current->getOpcode() != PlanGraphOpcode::VAR) {
            throw TuringException("PlanGraphTester: expected var node");
        }

        if (const auto* varNode = dynamic_cast<const VarNode*>(_current)) {
            const VarDecl* varDecl = varNode->getVarDecl();
            if (varDecl->getName() != var) {
                throw TuringException("PlanGraphTester: wrong var name");
            }
        } else {
            throw TuringException("PlanGraphTester: expected var node");
        }

        const auto& outs = _current->outputs();
        if (!outs.empty()) {
            _current = outs.front();
        } else {
            _current = nullptr;
        }

        return *this;
    }

    void validateComplete() {
        if (_current != nullptr) {
            throw TuringException("PlanGraphTester: not all nodes were tested");
        }
    }

private:
    const PlanGraphNode* _current {nullptr};
};

}
