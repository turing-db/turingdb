#pragma once

#include <memory>

#include "ArcManager.h"
#include "CommitData.h"

namespace db {

class GraphView;
class GraphReader;
class Graph;
class CommitBuilder;

class Transaction {
public:
    Transaction() = default;
    ~Transaction() = default;

    Transaction(const Graph& graph, const WeakArc<const CommitData>& data)
        : _graph(&graph),
          _data(data)
    {
    }

    Transaction(const Transaction&) = default;
    Transaction(Transaction&&) = default;
    Transaction& operator=(const Transaction&) = default;
    Transaction& operator=(Transaction&&) = default;

    [[nodiscard]] bool isValid() const { return _data != nullptr; }

    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;

private:
    const Graph* _graph {nullptr};
    WeakArc<const CommitData> _data;
};

class WriteTransaction {
public:
    WriteTransaction() = default;
    ~WriteTransaction() = default;

    WriteTransaction(Graph& graph, const WeakArc<const CommitData>& data)
        : _graph(&graph),
          _data(data)
    {
    }

    WriteTransaction(const WriteTransaction&) = default;
    WriteTransaction(WriteTransaction&&) = default;
    WriteTransaction& operator=(const WriteTransaction&) = default;
    WriteTransaction& operator=(WriteTransaction&&) = default;

    [[nodiscard]] bool isValid() const { return _data != nullptr; }

    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;

    [[nodiscard]] std::unique_ptr<CommitBuilder> prepareCommit() const;

private:
    Graph* _graph {nullptr};
    WeakArc<const CommitData> _data;
};

}

