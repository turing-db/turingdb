#include "TypingGraph.h"

#include "writers/GraphWriter.h"
#include "metadata/PropertyType.h"

using namespace db;

void TypingGraph::createTypingGraph(Graph* graph) {
    GraphWriter writer {graph};
    
    const auto typeHolder = writer.addNode({"Typer"});
    writer.addNodeProperty<types::Int64>(typeHolder, "pos_int", 256);
    writer.addNodeProperty<types::Int64>(typeHolder, "neg_int", -512);
    writer.addNodeProperty<types::Int64>(typeHolder, "zero_int", 0);

    writer.addNodeProperty<types::UInt64>(typeHolder, "uint", 333);

    writer.addNodeProperty<types::Double>(typeHolder, "dbl", 1.618);
    
    writer.addNodeProperty<types::String>(typeHolder, "str", "string property");

    writer.addNodeProperty<types::Bool>(typeHolder, "bool_t", true);
    writer.addNodeProperty<types::Bool>(typeHolder, "bool_f", false);

    writer.commit();

    const auto frend = writer.addNode({"Friend"});
    writer.commit();

    const auto typeFriend = writer.addEdge("FRIENDS_WITH", typeHolder, frend);
    writer.addEdgeProperty<types::String>(typeFriend, "name", "type has a friend");
    writer.commit();


    writer.submit();
}
