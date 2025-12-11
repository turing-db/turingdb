#pragma once

#include "Endpoints.h"
#include "HTTP.h"
#include "NetWriter.h"
#include "metadata/PropertyTypeMap.h"
#include "views/NodeView.h"
#include "QueryStatus.h"

#include "BioAssert.h"

namespace db {

class HTTPResponseWriter {
public:
    class Header {
    public:
        Header(const Header&) = delete;
        Header(Header&&) = delete;
        Header& operator=(const Header&) = delete;
        Header& operator=(Header&&) = delete;

        Header(HTTPResponseWriter& w,
               net::HTTP::Status status,
               bool keepAlive)
            : _w(w)
        {
            _w.writeHeader(status, keepAlive);
        }

        ~Header() {
            _w.flush();
        }

    private:
        HTTPResponseWriter& _w;
    };

    class Body {
    public:
        Body(const Body&) = delete;
        Body(Body&&) = delete;
        Body& operator=(const Body&) = delete;
        Body& operator=(Body&&) = delete;

        explicit Body(HTTPResponseWriter& w)
            : _w(w)
        {
            _w.write('{');
        }

        ~Body() {
            _w.write("}\n");
        }

    private:
        HTTPResponseWriter& _w;
    };

    class Data {
    public:
        Data(const Data&) = delete;
        Data(Data&&) = delete;
        Data& operator=(const Data&) = delete;
        Data& operator=(Data&&) = delete;

        explicit Data(HTTPResponseWriter& w)
            : _w(w)
        {
            _w.write("\"data\": ");
        }

        ~Data() {
            if (!_destroyed) {
                destroy();
            }
        }

        void destroy() {
            _destroyed = true;
        }

        void destoryAndAppendComma() {
            destroy();
            _w.write(',');
        }

    private:
        HTTPResponseWriter& _w;
        bool _destroyed = false;
    };

    class Array {
    public:
        Array(const Array&) = delete;
        Array(Array&&) = delete;
        Array& operator=(const Array&) = delete;
        Array& operator=(Array&&) = delete;

        explicit Array(HTTPResponseWriter& w)
            : _w(w)
        {
            _w.write('[');
        }

        ~Array() {
            if (!_destroyed) {
                destroy();
            }
        }

        void destroy() {
            _w.write(']');
            _destroyed = true;
        }

        void destoryAndAppendComma() {
            destroy();
            _w.write(',');
        }

    private:
        HTTPResponseWriter& _w;
        bool _destroyed = false;
    };

    class Object {
    public:
        Object(const Object&) = delete;
        Object(Object&&) = delete;
        Object& operator=(const Object&) = delete;
        Object& operator=(Object&&) = delete;

        explicit Object(HTTPResponseWriter& w)
            : _w(w)
        {
            _w.write('{');
        }

        ~Object() {
            if (!_destroyed) {
                destroy();
            }
        }

        void destroy() {
            _w.write('}');
            _destroyed = true;
        }

        void destroyAndAppendComma() {
            destroy();
            _w.write(',');
        }

    private:
        HTTPResponseWriter& _w;
        bool _destroyed = false;
    };

    class Error {
    public:
        Error(const Error&) = delete;
        Error(Error&&) = delete;
        Error& operator=(const Error&) = delete;
        Error& operator=(Error&&) = delete;

        explicit Error(HTTPResponseWriter& w)
            : _w(w)
        {
            _w.write("\"error\": \"");
        }

        ~Error() {
            _w.write('"');
        }

    private:
        HTTPResponseWriter& _w;
    };

    explicit HTTPResponseWriter(net::NetWriter* writer)
        : _writer(writer)
    {
    }

    HTTPResponseWriter& writeHeader(net::HTTP::Status status, bool keepAlive = true) {
        _writer->setFirstLine(status);
        _writer->addConnection(net::getConnectionHeader(!keepAlive));
        _writer->addChunkedTransferEncoding();
        _writer->addContentType(net::ContentType::JSON);
        _writer->endHeader();
        _writer->flushHeader();
        return *this;
    }

    HTTPResponseWriter& write(std::string_view data) {
        _writer->write(data);
        return *this;
    }

    HTTPResponseWriter& write(char c) {
        _writer->write(c);
        return *this;
    }

    void flush() { _writer->flush(); }

    [[nodiscard]] Header startHeader(net::HTTP::Status status, bool keepAlive = false) {
        return Header(*this, status, keepAlive);
    }

    [[nodiscard]] Body startBody() { return Body(*this); }
    [[nodiscard]] Data startData() { return Data(*this); }
    [[nodiscard]] Array startArray() { return Array(*this); }
    [[nodiscard]] Object startObject() { return Object(*this); }
    [[nodiscard]] Error startError() { return Error(*this); }

    template <typename ValueT>
    void writeKeyValue(std::string_view key, ValueT&& value) {
        writeValue(key);
        write(':');
        writeValue(std::forward<ValueT>(value));
    }

    template <std::integral KeyT, typename ValueT>
    void writeKeyValue(KeyT key, ValueT&& value) {
        write('"');
        writeValue(key);
        write("\":");
        writeValue(std::forward<ValueT>(value));
    }

    void writeKeyValue(const PropertyTypeMap& propTypes, std::string_view key, const NodeView& nodeView) {
        writeValue(key);
        write(':');
        writeValue(propTypes, nodeView);
    }

    template <std::integral KeyT>
    void writeKeyValue(const PropertyTypeMap& propTypes, KeyT key, const NodeView& nodeView) {
        write('"');
        writeValue(key);
        write("\":");
        writeValue(propTypes, nodeView);
    }

    void writeKeyValueOutEdge(std::string_view key, const EdgeRecord& edge) {
        writeValue(key);
        writeOutEdge(edge);
    }

    void writeKeyValueInEdge(std::string_view key, const EdgeRecord& edge) {
        writeValue(key);
        writeInEdge(edge);
    }

    void writeKeyValueOutEdge(std::integral auto key, const EdgeRecord& edge) {
        write('"');
        writeValue(key);
        write("\":");
        writeOutEdge(edge);
    }

    void writeKeyValueInEdge(std::integral auto key, const EdgeRecord& edge) {
        write('"');
        writeValue(key);
        write("\":");
        writeInEdge(edge);
    }

    void writeValue(std::string_view v) {
        write('"');
        write(v);
        write('"');
    }

    void writeValue(std::integral auto v) {
        write(std::to_string(v));
    }

    void writeValue(std::floating_point auto v) {
        write(std::to_string(v));
    }

    void writeValue(CustomBool v) {
        write(v._boolean ? "true" : "false");
    }

    void writeValue(bool v) {
        write(v ? "true" : "false");
    }

    void writeValue(const PropertyTypeMap& propTypes, const NodeView& nodeView) {
        const auto& edges = nodeView.edges();
        const auto& props = nodeView.properties();
        write('{');

        write(fmt::format("\"id\":{},", nodeView.nodeID()));
        write(fmt::format("\"in_edge_count\":{},", edges.getInEdgeCount()));
        write(fmt::format("\"out_edge_count\":{},", edges.getOutEdgeCount()));
        write("\"properties\":{");

        size_t count = 0;
        for (const auto& [ptID, value] : props) {
            std::visit([&](const auto& v) { writeKeyValue(propTypes.getName(ptID).value(), *v); }, value);
            ++count;

            if (count != props.getCount()) {
                write(',');
            }
        }
        write("}}");
    }

    enum class EdgeDir {
        OUT = 0,
        IN,
    };

    void writeOutEdge(const EdgeRecord& edge) {
        write('{');
        writeKeyValue("id", edge._edgeID.getValue());
        write(',');
        writeKeyValue("src", edge._nodeID.getValue());
        write(',');
        writeKeyValue("tgt", edge._otherID.getValue());
        write('}');
    }

    void writeInEdge(const EdgeRecord& edge) {
        write('{');
        writeKeyValue("id", edge._edgeID.getValue());
        write(',');
        writeKeyValue("tgt", edge._nodeID.getValue());
        write(',');
        writeKeyValue("src", edge._otherID.getValue());
        write('}');
    }

    void writeHttpError(net::HTTP::Status status) {
        bioassert(status != net::HTTP::Status::OK, "HttpStatus is OK but trying to return an error");
        writeHeader(status).flush();
    }

    void writeQueryError(const QueryStatus& status) {
        bioassert(!status.isOk(), "QueryStatus is OK but trying to return an error");
        write(QueryStatusDescription::value(status.getStatus()));
    }

    void writeEndpointError(EndpointStatus status) {
        bioassert(status != EndpointStatus::OK, "EndpointStatus is OK but trying to return an error");
        write(EndpointStatusDescription::value(status));
    }

    net::NetWriter* getWriter() { return _writer; }

private:
    net::NetWriter* _writer {nullptr};
};

}
