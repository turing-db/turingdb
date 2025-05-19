#pragma once

#include "ArcManager.h"
#include "CommitData.h"
#include "Change.h"

namespace db {

class GraphView;
class GraphReader;
class VersionController;
class CommitBuilder;
class DataPartBuilder;

class Transaction {
public:
    Transaction();
    ~Transaction();

    Transaction(const WeakArc<const CommitData>& data)
        : _data(data) {
    }

    Transaction(const Transaction&) = default;
    Transaction(Transaction&&) = default;
    Transaction& operator=(const Transaction&) = default;
    Transaction& operator=(Transaction&&) = default;

    [[nodiscard]] bool isValid() const { return _data != nullptr; }

    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;
    [[nodiscard]] const WeakArc<const CommitData>& commitData() const { return _data; }

private:
    WeakArc<const CommitData> _data;
};

class WriteTransaction {
public:
    WriteTransaction();
    ~WriteTransaction();

    WriteTransaction(const WeakArc<const CommitData>& data,
                     CommitBuilder* builder,
                     DataPartBuilder* partBuilder,
                     Change::Accessor& changeAccessor);

    WriteTransaction(const WriteTransaction&) = delete;
    WriteTransaction(WriteTransaction&&) = default;
    WriteTransaction& operator=(const WriteTransaction&) = delete;
    WriteTransaction& operator=(WriteTransaction&&) = default;

    [[nodiscard]] bool isValid() const { return _data != nullptr; }

    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;
    [[nodiscard]] const CommitData& commitData() const { return *_data; }
    [[nodiscard]] CommitBuilder* builder() const { return _builder; }
    [[nodiscard]] DataPartBuilder* partBuilder() const { return _partBuilder; }
    [[nodiscard]] Change::Accessor& changeAccessor() { return _changeAccessor; }

private:
    WeakArc<const CommitData> _data;
    CommitBuilder* _builder {nullptr};
    DataPartBuilder* _partBuilder {nullptr};
    Change::Accessor _changeAccessor;
};

}

