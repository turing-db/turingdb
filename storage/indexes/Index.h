#pragma once

#include "ID.h"

#include "views/GraphView.h"

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
     */
    virtual void boundedQuery(const Column* query,
                              Column* result,
                              QueryState& state,
                              size_t limit) const = 0;

    virtual size_t size() const = 0;
    virtual PropertyTypeID property() const = 0;
    virtual bool isNodeIndex() const = 0;

    std::string_view name() const { return _name; }

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
