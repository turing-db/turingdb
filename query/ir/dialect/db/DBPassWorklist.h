#pragma once

#include <stddef.h>
#include <vector>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "llvm/ADT/DenseMap.h"

#include "DBOps.h"

namespace mlir::db {

// The ops of one kind a pass has left to try. It listens to the rewriter, so it queues every
// such op a rewrite creates and drops every op a rewrite erases.
template <typename... OpTypes>
class OpWorklist : public mlir::RewriterBase::Listener {
public:
    explicit OpWorklist(Operation* root) {
        root->walk([this](Operation* op) {
            if (isa<OpTypes...>(op)) {
                push(op);
            }
        });
    }

    Operation* pop() {
        while (_next < _ops.size()) {
            Operation* const op = _ops[_next++];
            if (op) {
                _positions.erase(op);
                return op;
            }
        }

        return nullptr;
    }

    void notifyOperationInserted(Operation* op, mlir::OpBuilder::InsertPoint previous) override {
        if (isa<OpTypes...>(op)) {
            push(op);
        }
    }

    void notifyOperationErased(Operation* op) override {
        const auto found = _positions.find(op);
        if (found != _positions.end()) {
            _ops[found->second] = nullptr;
            _positions.erase(found);
        }
    }

private:
    std::vector<Operation*> _ops;
    llvm::DenseMap<Operation*, size_t> _positions;
    size_t _next {0};

    void push(Operation* op) {
        if (_positions.try_emplace(op, _ops.size()).second) {
            _ops.push_back(op);
        }
    }
};

// A match can read IR far above its op, so a rewrite can make an op it never touched match.
// The worklist runs again until a round rewrites nothing, as MLIR's greedy driver does.
template <typename... OpTypes, typename RewriteOp>
void runWorklist(Operation* root, RewriteOp rewriteOp) {
    bool rewritten = true;
    while (rewritten) {
        rewritten = false;

        OpWorklist<OpTypes...> worklist(root);
        mlir::IRRewriter rewriter(root->getContext(), &worklist);

        while (Operation* op = worklist.pop()) {
            if (rewriteOp(op, rewriter)) {
                rewritten = true;
            }
        }
    }
}

template <typename Match, typename MatchFilter, typename RewriteFilter>
void runFilterWorklist(Operation* root, MatchFilter matchFilter, RewriteFilter rewriteFilter) {
    runWorklist<FilterOp>(root, [&matchFilter, &rewriteFilter](Operation* op, mlir::RewriterBase& rewriter) {
        Match match;
        if (!matchFilter(cast<FilterOp>(op), match)) {
            return false;
        }

        rewriteFilter(match, rewriter);
        return true;
    });
}

}
