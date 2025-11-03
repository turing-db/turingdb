#pragma once

#include <variant>

#include "ArcManager.h"
#include "CommitData.h"
#include "ChangeAccessor.h"

namespace db {

class GraphView;
class GraphReader;
class VersionController;
class CommitBuilder;
class DataPartBuilder;

class FrozenCommitTx {
public:
    FrozenCommitTx();
    ~FrozenCommitTx();

    FrozenCommitTx(const WeakArc<const CommitData>& data)
        : _data(data)
    {
    }

    FrozenCommitTx(const FrozenCommitTx&) = default;
    FrozenCommitTx(FrozenCommitTx&&) = default;
    FrozenCommitTx& operator=(const FrozenCommitTx&) = default;
    FrozenCommitTx& operator=(FrozenCommitTx&&) = default;

    [[nodiscard]] bool isValid() const { return _data != nullptr; }

    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;
    [[nodiscard]] const WeakArc<const CommitData>& commitData() const { return _data; }

private:
    WeakArc<const CommitData> _data;
};

class PendingCommitReadTx {
public:
    PendingCommitReadTx();
    ~PendingCommitReadTx();

    PendingCommitReadTx(ChangeAccessor&& changeAccessor,
                        const CommitBuilder* commitBuilder);

    PendingCommitReadTx(const PendingCommitReadTx&) = delete;
    PendingCommitReadTx(PendingCommitReadTx&&) = default;
    PendingCommitReadTx& operator=(const PendingCommitReadTx&) = delete;
    PendingCommitReadTx& operator=(PendingCommitReadTx&&) = default;

    [[nodiscard]] const ChangeAccessor& changeAccessor() const { return _changeAccessor; }
    [[nodiscard]] const CommitBuilder* commitBuilder() const { return _commitBuilder; }
    [[nodiscard]] bool isValid() const { return _changeAccessor.isValid(); }
    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;

private:
    ChangeAccessor _changeAccessor;
    const CommitBuilder* _commitBuilder {nullptr};
};

class PendingCommitWriteTx {
public:
    PendingCommitWriteTx();
    ~PendingCommitWriteTx();

    PendingCommitWriteTx(ChangeAccessor&& changeAccessor,
                         CommitBuilder* commitBuilder);

    PendingCommitWriteTx(const PendingCommitWriteTx&) = delete;
    PendingCommitWriteTx(PendingCommitWriteTx&&) = default;
    PendingCommitWriteTx& operator=(const PendingCommitWriteTx&) = delete;
    PendingCommitWriteTx& operator=(PendingCommitWriteTx&&) = default;

    [[nodiscard]] ChangeAccessor& changeAccessor() { return _changeAccessor; }
    [[nodiscard]] CommitBuilder* commitBuilder() { return _commitBuilder; }
    [[nodiscard]] bool isValid() const { return _changeAccessor.isValid(); }
    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;

private:
    ChangeAccessor _changeAccessor;
    CommitBuilder* _commitBuilder {nullptr};
};

template <typename T>
concept TransactionType = std::is_same_v<T, FrozenCommitTx>
                       || std::is_same_v<T, PendingCommitWriteTx>
                       || std::is_same_v<T, PendingCommitReadTx>;

class Transaction {
public:
    Transaction() = default;

    template <TransactionType T>
    Transaction(T&& tx)
        : _tx(std::forward<T>(tx))
    {
    }

    ~Transaction() = default;

    Transaction(const Transaction&) = delete;
    Transaction(Transaction&&) = default;
    Transaction& operator=(const Transaction&) = delete;
    Transaction& operator=(Transaction&&) = default;

    template <TransactionType T>
    [[nodiscard]] T& get() {
        return std::get<T>(_tx);
    }

    template <TransactionType T>
    [[nodiscard]] const T& get() const {
        return std::get<T>(_tx);
    }

    [[nodiscard]] bool readingFrozenCommit() const {
        return std::holds_alternative<FrozenCommitTx>(_tx);
    }

    [[nodiscard]] bool readingPendingCommit() const {
        return std::holds_alternative<PendingCommitReadTx>(_tx);
    }

    [[nodiscard]] bool writingPendingCommit() const {
        return std::holds_alternative<PendingCommitWriteTx>(_tx);
    }

    [[nodiscard]] bool isValid() const;
    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;

private:
    std::variant<FrozenCommitTx, PendingCommitWriteTx, PendingCommitReadTx> _tx;
};

}

