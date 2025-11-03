#include "SimpleGraph.h"

#include "Graph.h"
#include "ID.h"
#include "reader/GraphReader.h"
#include "metadata/PropertyType.h"
#include "metadata/LabelSet.h"
#include "writers/GraphWriter.h"
#include "versioning/Transaction.h"

using namespace db;

NodeID SimpleGraph::findNodeID(Graph* graph, std::string_view nodeName) {
    const auto transaction = graph->openTransaction();
    const auto reader = transaction.readGraph();

    auto it = reader.scanNodeProperties<types::String>(0).begin();
    for (; it.isValid(); it.next()) {
        if (it.get() == nodeName) {
            return it.getCurrentNodeID();
        }
    }

    throw std::runtime_error(fmt::format("Node not found: {}", nodeName));
}

void SimpleGraph::findNodesByLabel(Graph* graph, std::string_view labelName, std::vector<NodeID>& nodeIDs) {
    nodeIDs.clear();

    const auto transaction = graph->openTransaction();
    const auto reader = transaction.readGraph();

    LabelSet labelSet;
    const LabelID labelID = reader.getMetadata().labels().get(labelName).value();
    labelSet.set(labelID);
    const LabelSetHandle labelSetHandle = LabelSetHandle(labelSet);

    auto it = reader.scanNodesByLabel(labelSetHandle).begin();
    for (; it.isValid(); it.next()) {
        nodeIDs.push_back(it.get());
    }
}

void SimpleGraph::findOutEdges(Graph* graph,
                               const std::vector<NodeID>& nodeIDs,
                               std::vector<EdgeID>& edgeIDs,
                               std::vector<EdgeTypeID>& edgeTypes,
                               std::vector<NodeID>& targetNodeIDs) {
    edgeIDs.clear();
    edgeTypes.clear();
    targetNodeIDs.clear();
                                
    const auto transaction = graph->openTransaction();
    const auto reader = transaction.readGraph();

    ColumnNodeIDs columnNodeIDs(nodeIDs);

    const auto edges = reader.getOutEdges(&columnNodeIDs);
    for (const auto& edge : edges) {
        edgeIDs.push_back(edge._edgeID);
        edgeTypes.push_back(edge._edgeTypeID);
        targetNodeIDs.push_back(edge._otherID);
    }
}

void SimpleGraph::createSimpleGraph(Graph* graph) {
    GraphWriter writer {graph};
    writer.setName("simpledb");

    auto findNodeIDInWriter = [&](std::string_view nodeName) -> NodeID {
        const auto transaction = writer.openWriteTransaction();
        const auto reader = transaction.readGraph();
        
        auto it = reader.scanNodeProperties<types::String>(0).begin();
        for (; it.isValid(); it.next()) {
            if (it.get() == nodeName) {
                return it.getCurrentNodeID();
            }
        }

        throw std::runtime_error(fmt::format("Node not found: {}", nodeName));
    };

    // 0
    const auto remy = writer.addNode({"Person", "SoftwareEngineering", "Founder"});
    writer.addNodeProperty<types::String>(remy, "name", "Remy");
    writer.addNodeProperty<types::String>(remy, "dob", "18/01");
    writer.addNodeProperty<types::Int64>(remy, "age", 32);
    writer.addNodeProperty<types::Bool>(remy, "isFrench", true);
    writer.addNodeProperty<types::Bool>(remy, "hasPhD", true);

    // 1
    const auto adam = writer.addNode({"Person", "Bioinformatics", "Founder"});
    writer.addNodeProperty<types::String>(adam, "name", "Adam");
    writer.addNodeProperty<types::String>(adam, "dob", "18/08");
    writer.addNodeProperty<types::Int64>(adam, "age", 32);
    writer.addNodeProperty<types::Bool>(adam, "isFrench", true);
    writer.addNodeProperty<types::Bool>(adam, "hasPhD", true);

    // 2
    const auto computers = writer.addNode({"Interest", "SoftwareEngineering"});
    writer.addNodeProperty<types::String>(computers, "name", "Computers");
    writer.addNodeProperty<types::Bool>(computers, "isReal", true);

    // 3
    const auto eighties = writer.addNode({"Interest", "Exotic"});
    writer.addNodeProperty<types::String>(eighties, "name", "Eighties");
    writer.addNodeProperty<types::Bool>(eighties, "isReal", false);

    // 4
    const auto bio = writer.addNode({"Interest"});
    writer.addNodeProperty<types::String>(bio, "name", "Bio");

    // 5
    const auto cooking = writer.addNode({"Interest"});
    writer.addNodeProperty<types::String>(cooking, "name", "Cooking");

    // 6
    const auto ghosts = writer.addNode({"Interest", "Supernatural", "SleepDisturber", "Exotic"}); //6
    writer.addNodeProperty<types::String>(ghosts, "name", "Ghosts");
    writer.addNodeProperty<types::Bool>(ghosts, "isReal", true);

    // 0,1
    const auto remyAdam = writer.addEdge("KNOWS_WELL", remy, adam);
    writer.addEdgeProperty<types::String>(remyAdam, "name", "Remy -> Adam");
    writer.addEdgeProperty<types::Int64>(remyAdam, "duration", 20);

    // 1,0
    const auto adamRemy = writer.addEdge("KNOWS_WELL", adam, remy);
    writer.addEdgeProperty<types::String>(adamRemy, "name", "Adam -> Remy");
    writer.addEdgeProperty<types::Int64>(adamRemy, "duration", 20);

    // 0,6
    const auto remyGhosts = writer.addEdge("INTERESTED_IN", remy, ghosts);
    writer.addEdgeProperty<types::String>(remyGhosts, "name", "Remy -> Ghosts");
    writer.addEdgeProperty<types::Int64>(remyGhosts, "duration", 20);
    writer.addEdgeProperty<types::String>(remyGhosts, "proficiency", "expert");

    // 0,2
    const auto remyComputers = writer.addEdge("INTERESTED_IN", remy, computers);
    writer.addEdgeProperty<types::String>(remyComputers, "name", "Remy -> Computers");
    writer.addEdgeProperty<types::String>(remyComputers, "proficiency", "expert");

    // 0,3
    const auto remyEighties = writer.addEdge("INTERESTED_IN", remy, eighties);
    writer.addEdgeProperty<types::String>(remyEighties, "name", "Remy -> Eighties");
    writer.addEdgeProperty<types::Int64>(remyEighties, "duration", 20);
    writer.addEdgeProperty<types::String>(remyEighties, "proficiency", "moderate");

    // 1,4
    const auto adamBio = writer.addEdge("INTERESTED_IN", adam, bio);
    writer.addEdgeProperty<types::String>(adamBio, "name", "Adam -> Bio");

    // 1,5
    const auto adamCooking = writer.addEdge("INTERESTED_IN", adam, cooking);
    writer.addEdgeProperty<types::String>(adamCooking, "name", "Adam -> Cooking");

    // 6,0
    const auto ghostsRemy = writer.addEdge("KNOWS_WELL", ghosts, remy);
    writer.addEdgeProperty<types::String>(ghostsRemy, "name", "Ghosts -> Remy");
    writer.addEdgeProperty<types::String>(ghostsRemy, "proficiency", "expert");
    writer.addEdgeProperty<types::Int64>(ghostsRemy, "duration", 200);

    writer.commit();

    // 7
    const auto maxime = writer.addNode({"Person", "Bioinformatics"});
    writer.addNodeProperty<types::String>(maxime, "name", "Maxime");
    writer.addNodeProperty<types::String>(maxime, "dob", "24/07");
    writer.addNodeProperty<types::Bool>(maxime, "isFrench", true);
    writer.addNodeProperty<types::Bool>(maxime, "hasPhD", false);

    // 8
    const auto paddle = writer.addNode({"Interest"});
    writer.addNodeProperty<types::String>(paddle, "name", "Paddle");

    
    const auto maximeBio = writer.addEdge("INTERESTED_IN", maxime, findNodeIDInWriter("Bio"));
    writer.addEdgeProperty<types::String>(maximeBio, "name", "Maxime -> Bio");

    const auto maximePaddle = writer.addEdge("INTERESTED_IN", maxime, paddle);
    writer.addEdgeProperty<types::String>(maximePaddle, "name", "Maxime -> Paddle");
    writer.addEdgeProperty<types::String>(maximePaddle, "proficiency", "expert");

    writer.commit();

    // 9
    const auto luc = writer.addNode({"Person", "SoftwareEngineering"});
    writer.addNodeProperty<types::String>(luc, "dob", "28/05");
    writer.addNodeProperty<types::String>(luc, "name", "Luc");
    writer.addNodeProperty<types::Bool>(luc, "isFrench", true);
    writer.addNodeProperty<types::Bool>(luc, "hasPhD", true);

    // 10
    const auto animals = writer.addNode({"Interest", "SleepDisturber"});
    writer.addNodeProperty<types::String>(animals, "name", "Animals");
    writer.addNodeProperty<types::Bool>(animals, "isReal", true);

    const auto lucAnimals = writer.addEdge("INTERESTED_IN", luc, animals);
    writer.addEdgeProperty<types::String>(lucAnimals, "name", "Luc -> Animals");
    writer.addEdgeProperty<types::Int64>(lucAnimals, "duration", 20);

    const auto lucComputers = writer.addEdge("INTERESTED_IN", luc, findNodeIDInWriter("Computers"));
    writer.addEdgeProperty<types::String>(lucComputers, "name", "Luc -> Computers");
    writer.addEdgeProperty<types::Int64>(lucComputers, "duration", 15);

    writer.commit();

    // 11
    const auto martina = writer.addNode({"Person", "Bioinformatics"});
    writer.addNodeProperty<types::String>(martina, "name", "Martina");
    writer.addNodeProperty<types::Bool>(martina, "isFrench", false);
    writer.addNodeProperty<types::Bool>(martina, "hasPhD", true);

    const auto martinaCooking = writer.addEdge("INTERESTED_IN", martina, findNodeIDInWriter("Cooking"));
    writer.addEdgeProperty<types::String>(martinaCooking, "name", "Martina -> Cooking");
    writer.addEdgeProperty<types::Int64>(martinaCooking, "duration", 10);

    writer.commit();

    // 12
    const auto suhas = writer.addNode({"Person", "SoftwareEngineering"});
    writer.addNodeProperty<types::String>(suhas, "name", "Suhas");
    writer.addNodeProperty<types::Bool>(suhas, "isFrench", false);
    writer.addNodeProperty<types::Bool>(suhas, "hasPhD", false);

    writer.submit();
}
