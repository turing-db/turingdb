#pragma once

#include "HTTPResponseWriter.h"

namespace net {

class TCPConnection;
class AbstractThreadContext;

namespace HTTP { struct Info; }

}

namespace db {

class DBThreadContext;
class TuringDB;
class Graph;

class DBServerProcessor {
public:
    DBServerProcessor(TuringDB& db,
                      net::TCPConnection& connection);
    ~DBServerProcessor();

    void process(net::AbstractThreadContext*);

private:
    const struct Endpoints {
        DBServerProcessor* _session {nullptr};
    } _endpoints {this};

    HTTPResponseWriter _writer;
    TuringDB& _db;
    net::TCPConnection& _connection;
    DBThreadContext* _threadContext {nullptr};

    const Graph* getRequestedGraph() const;
    const net::HTTP::Info& getHttpInfo() const;

    void query();
    void load_graph();
    void get_graph_status();
    void is_graph_loaded();
    void is_graph_loading();
    void list_loaded_graphs();
    void list_avail_graphs();
    void list_labels();
    void list_property_types();
    void list_edge_types();
    void list_nodes();
    void get_node_properties();
    void get_neighbors();
    void get_nodes();
    void get_node_edges();
    void get_edges();
    void explore_node_edges();
};

}
