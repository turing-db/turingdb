#pragma once

#include "ID.h"

#include "views/GraphView.h"

#include "columns/ColumnIndices.h"

namespace db {

class Column;

class Index {
public:
    struct QueryState;

    explicit Index(std::string_view name)
        : _name(name)
    {
    }

    virtual ~Index() = default;

    Index(const Index&) = delete;
    Index(Index&&) = delete;
    Index& operator=(const Index&) = delete;
    Index& operator=(Index&&) = delete;

    virtual void init(GraphView view) = 0;

    /**
     * @brief A generic interface to recieve an unbounded number of results in
     * @param result from a query.
     */
    virtual void query(const Column* query, Column* result) const = 0;

    /**
     * @brief A generic interface to recieve a bounded number of results in @param result
     * from a stateful query.
     * @detail Indices in @param indices are use to track correspondence between which
     * rows in the @param query column produces which rows in the @ref result column.
     * @param indices may be used in @ref MaterializeProcessor during the pipeline.
     */
    virtual void boundedQuery(const Column* query,
                              Column* result,
                              ColumnIndices* indices,
                              QueryState& state,
                              size_t limit) const = 0;

    virtual size_t size() const = 0;
    virtual PropertyTypeID property() const = 0;
    virtual consteval bool isNodeIndex() const = 0;

    std::string_view name() const { return _name; }

    /// Struct to track state in calls to @ref boundedQuery
    struct QueryState {
        void reset() {
            _keyIndex = 0;
            _valueIndex = 0;
            _finished = false;
        }
        size_t _keyIndex {0};
        size_t _valueIndex {0};
        bool _finished {false};
    };

protected:
    std::string _name;
    bool _initialised {false};
};

}
