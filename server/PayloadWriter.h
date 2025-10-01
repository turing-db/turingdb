#pragma once

#include <vector>

#include "ID.h"
#include "metadata/GraphMetadata.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "views/NodeView.h"
#include "views/EdgeView.h"
#include "metadata/PropertyType.h"
#include "NetWriter.h"

namespace db {

template <typename T>
concept JsonPrimitive = std::same_as<uint64_t, T>
                     || std::same_as<uint32_t, T>
                     || std::same_as<uint16_t, T>
                     || std::same_as<uint8_t, T>

                     || std::same_as<int64_t, T>
                     || std::same_as<int32_t, T>
                     || std::same_as<int16_t, T>
                     || std::same_as<int8_t, T>

                     || std::same_as<size_t, T>

                     || std::same_as<float, T>
                     || std::same_as<float, T>
                     || std::same_as<double, T>
                     || std::same_as<bool, T>
                     || std::same_as<CustomBool, T>
                     || std::same_as<EntityID, T>
                     || std::same_as<NodeID, T>
                     || std::same_as<EdgeID, T>
                     || std::same_as<LabelSetID, T>
                     || std::same_as<PropertyTypeID, T>
                     || std::same_as<LabelID, T>;

template <typename T>
concept JsonKey = (JsonPrimitive<T>)
               && !std::same_as<T, bool>
               && !std::same_as<T, CustomBool>
               && !std::same_as<T, NodeView>
               && !std::same_as<T, EdgeRecord>;

enum class EdgeDir : uint8_t {
    Out = 0,
    In,
};

class PayloadWriter {
public:
    explicit PayloadWriter(net::NetWriter* writer, const GraphMetadata* metadata = nullptr)
        : _writer(writer),
        _metadata(metadata)
    {
    }

    PayloadWriter(const PayloadWriter&) = delete;
    PayloadWriter(PayloadWriter&&) = delete;
    PayloadWriter& operator=(const PayloadWriter&) = delete;
    PayloadWriter& operator=(PayloadWriter&&) = delete;

    ~PayloadWriter() {
        finish();
    }

    void setMetadata(const GraphMetadata* metadata) {
        _metadata = metadata;
    }

    void obj() {
        if (_comma) {
            _writer->write(',');
        }
        _writer->write('{');
        _closingTokens.push_back('}');
        _comma = false;
    }

    void arr() {
        if (_comma) {
            _writer->write(',');
        }
        _writer->write('[');
        _closingTokens.push_back(']');
        _comma = false;
    }

    void end() {
        _writer->write(_closingTokens.back());
        _closingTokens.pop_back();
        _comma = true;
    }

    void key(std::string_view k) {
        if (_comma) {
            _writer->write(",\"");
            _comma = false;
        } else {
            _writer->write('"');
        }
        write(k);
        _writer->write("\":");
    }

    void key(JsonKey auto k) {
        if (_comma) {
            _writer->write(",\"");
            _comma = false;
        } else {
            _writer->write('"');
        }
        write(k);
        _writer->write("\":");
    }

    void value(JsonPrimitive auto v) {
        if (_comma) {
            _writer->write(',');
        }
        write(v);
        _comma = true;
    }

    void value(const NodeView& v) {
        write(v);
        _comma = true;
    }

    void value(const EdgeView& v) {
        write(v);
        _comma = true;
    }

    template <EdgeDir DirT>
    void value(const EdgeRecord& edge) {
        if (_comma) {
            _writer->write(',');
        }
        write<DirT>(edge);
        _comma = true;
    }

    void value(std::string_view v) {
        if (_comma) {
            _writer->write(",\"");
        } else {
            _writer->write('"');
        }
        write(v);
        write('"');
        _comma = true;
    }

    void value(const CommitBuilder* v) {
        value(v->hash().get());
    }

    void nullValue() {
        if (_comma) {
            _writer->write(",null");
        } else {
            _writer->write("null");
        }

        _comma = true;
    }

    template <typename T>
    void value(const std::optional<T>& v) {
        if (v) {
            value(*v);
        } else {
            nullValue();
        }
    }

    void value(const Change* v) {
        value(v->id().get());
    }

    void finish() {
        for (auto it = _closingTokens.rbegin(); it != _closingTokens.rend(); it++) {
            write(*it);
        }

        _comma = false;
        _closingTokens.clear();
    }

private:
    net::NetWriter* _writer {nullptr};
    const GraphMetadata* _metadata {nullptr};
    std::vector<char> _closingTokens;
    bool _comma = false;

    void write(std::signed_integral auto v) {
        _writer->write(v);
    }

    void write(std::unsigned_integral auto v) {
        _writer->write(v);
    }

    void write(std::floating_point auto v) {
        _writer->write(v);
    }

    void write(std::string_view v) {
        _writer->write(v);
    }

    void write(CustomBool v) {
        _writer->write(v._boolean ? "true" : "false");
    }

    void write(bool v) {
        _writer->write(v ? "true" : "false");
    }

    template <std::integral T, int I>
    void write(db::ID<T, I> id) {
        _writer->write(id.getValue());
    }

    void write(const NodeView& n) {
        bioassert(_metadata);
        const auto& propTypes = _metadata->propTypes();
        const auto& labels = _metadata->labels();

        std::vector<LabelID> labelIDs;

        const auto& edges = n.edges();
        const auto& props = n.properties();
        n.labelset().decompose(labelIDs);

        this->obj();

        // NodeID
        this->key("id");
        this->value(n.nodeID());

        // Node labels
        {
            this->key("labels");
            this->arr();
            for (const LabelID id : labelIDs) {
                this->value(labels.getName(id));
            }
            this->end();
        }

        // In edge count
        this->key("in_edge_count");
        this->value(edges.getInEdgeCount());

        // Out edge count
        this->key("out_edge_count");
        this->value(edges.getOutEdgeCount());

        // Properties
        {
            this->key("properties");
            this->obj();
            for (const auto& [ptID, variant] : props) {
                std::visit(
                    [&](const auto& v) {
                        this->key(propTypes.getName(ptID).value());
                        this->value(*v);
                    },
                    variant);
            }
            this->end();
        }

        this->end();
    }

    void write(const EdgeView& e) {
        bioassert(_metadata);

        const auto& props = e.properties();

        this->arr();

        this->value(e.id());
        this->value(e.sourceID());
        this->value(e.targetID());
        this->value(e.edgeTypeID());

        // Properties
        {
            this->obj();
            for (const auto& [ptID, variant] : props) {
                std::visit(
                    [&](const auto& v) {
                        this->key(ptID);
                        this->value(*v);
                    },
                    variant);
            }
            this->end();
        }

        this->end();
    }

    template <EdgeDir DirT>
    void write(const EdgeRecord& edge) {
        if constexpr (DirT == EdgeDir::Out) {
            this->obj();
            this->key("id");
            this->value(edge._edgeID);
            this->key("src");
            this->value(edge._nodeID);
            this->key("tgt");
            this->value(edge._otherID);
            this->end();
        } else if constexpr (DirT == EdgeDir::In) {
            this->obj();
            this->key("id");
            this->value(edge._edgeID);
            this->key("tgt");
            this->value(edge._nodeID);
            this->key("src");
            this->value(edge._otherID);
            this->end();
        } else {
            COMPILE_ERROR("Missing edge direction");
        }
    }
};

}
