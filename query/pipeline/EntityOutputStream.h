#pragma once

#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#include "PipelineException.h"
#include "dataframe/ColumnTag.h"

namespace db {

class EntityOutputStream {
public:
    struct NodeStream {
        ColumnTag _nodeIDsTag;
    };

    struct EdgeStream {
        ColumnTag _edgeIDsTag;
        ColumnTag _otherIDsTag;
        ColumnTag _edgeTypesTag;
    };

    using StreamVariant = std::variant<std::monostate, NodeStream, EdgeStream>;

    EntityOutputStream() = default;

    static EntityOutputStream createEmptyStream() {
        return EntityOutputStream {};
    }

    static EntityOutputStream createNodeStream(ColumnTag nodeIDsTag) {
        EntityOutputStream stream;
        stream._stream = NodeStream {nodeIDsTag};
        return stream;
    }

    static EntityOutputStream createEdgeStream(ColumnTag edgeIDsTag,
                                               ColumnTag otherIDsTag,
                                               ColumnTag edgeTypesTag) {
        EntityOutputStream stream;
        stream._stream = EdgeStream {edgeIDsTag, otherIDsTag, edgeTypesTag};
        return stream;
    }

    void projectEdgeTarget() {
        if (isNodeStream()) {
            return; // Already projected
        }
        if (!isEdgeStream()) {
            throw PipelineException("Cannot project edge target on non-edge stream");
        }

        _stream = NodeStream {asEdgeStream()._otherIDsTag};
    }

    bool isEmpty() const {
        return std::holds_alternative<std::monostate>(_stream);
    }

    bool isNodeStream() const {
        return std::holds_alternative<NodeStream>(_stream);
    }

    bool isEdgeStream() const {
        return std::holds_alternative<EdgeStream>(_stream);
    }

    const NodeStream& asNodeStream() const {
        return std::get<NodeStream>(_stream);
    }

    const EdgeStream& asEdgeStream() const {
        return std::get<EdgeStream>(_stream);
    }

    // Args can be any other input parameters letting us do more things with streams.
    template <typename VisitorFunc, typename... Args>
        requires requires(VisitorFunc visitor) {
            visitor(std::declval<const NodeStream&>(), std::declval<const Args&>()...);
            visitor(std::declval<const EdgeStream&>(), std::declval<const Args&>()...);
        }
    struct StreamVisitor {
        VisitorFunc _visitor;
        std::tuple<Args...> _args;

        using ReturnType = decltype(std::declval<VisitorFunc&>()(
            std::declval<const NodeStream&>(),
            std::declval<const Args&>()...));

        [[noreturn]] ReturnType operator()(const std::monostate&) const {
            throw std::runtime_error("Called StreamVisitor::operator() on empty stream");
        }

        template <typename Stream>
        ReturnType operator()(const Stream& stream) const {
            return std::apply(
                [&](const auto&... args) -> ReturnType {
                    return _visitor(stream, args...);
                },
                _args);
        }
    };

    template <typename Visitor, typename... Args>
    auto visit(Visitor&& visitor, Args&&... args) const {
        return std::visit(
            StreamVisitor<std::decay_t<Visitor>, std::decay_t<Args>...> {
                std::forward<Visitor>(visitor),
                std::tuple<std::decay_t<Args>...> {std::forward<Args>(args)...}},
            _stream);
    }

private:
    StreamVariant _stream;
};

}
