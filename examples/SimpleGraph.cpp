#include "SimpleGraph.h"

#include "TuringDB.h"
#include "SystemManager.h"
#include "Graph.h"
#include "EntityID.h"
#include "types/PropertyType.h"
#include "writers/GraphWriter.h"

using namespace db;

void SimpleGraph::createSimpleGraph(TuringDB& db) {
    auto& system = db.getSystemManager();
    Graph* graph = system.getDefaultGraph();
    GraphWriter writer {graph};

    auto remy = writer.addNode({"Person", "SoftwareEngineering", "Founder"});
    writer.addNodeProperty<types::String>(remy, "name", "Remy");
    writer.addNodeProperty<types::String>(remy, "dob", "18/01");
    writer.addNodeProperty<types::Bool>(remy, "isFrench", true);
    writer.addNodeProperty<types::Bool>(remy, "hasPhD", true);

    auto adam = writer.addNode({"Person", "Bioinformatics", "Founder"});
    writer.addNodeProperty<types::String>(adam, "name", "Adam");
    writer.addNodeProperty<types::String>(adam, "dob", "18/08");
    writer.addNodeProperty<types::Bool>(adam, "isFrench", true);
    writer.addNodeProperty<types::Bool>(adam, "hasPhD", true);

    auto computers = writer.addNode({"Interest", "SoftwareEngineering"});
    writer.addNodeProperty<types::String>(computers, "name", "Computers");
    writer.addNodeProperty<types::Bool>(computers, "isReal", true);

    auto eighties = writer.addNode({"Interest", "Exotic"});
    writer.addNodeProperty<types::String>(eighties, "name", "Eighties");
    writer.addNodeProperty<types::Bool>(eighties, "isReal", false);

    auto bio = writer.addNode({"Interest"});
    writer.addNodeProperty<types::String>(bio, "name", "Bio");

    auto cooking = writer.addNode({"Interest"});
    writer.addNodeProperty<types::String>(cooking, "name", "Cooking");

    auto ghosts = writer.addNode({"Interest", "Supernatural", "SleepDisturber", "Exotic"});
    writer.addNodeProperty<types::String>(ghosts, "name", "Ghosts");
    writer.addNodeProperty<types::Bool>(ghosts, "isReal", true);

    auto remyAdam = writer.addEdge("KNOWS_WELL", remy, adam);
    writer.addEdgeProperty<types::String>(remyAdam, "name", "Remy -> Adam");
    writer.addEdgeProperty<types::Int64>(remyAdam, "duration", 20);

    auto adamRemy = writer.addEdge("KNOWS_WELL", adam, remy);
    writer.addEdgeProperty<types::String>(adamRemy, "name", "Adam -> Remy");
    writer.addEdgeProperty<types::Int64>(adamRemy, "duration", 20);

    auto remyGhosts = writer.addEdge("INTERESTED_IN", remy, ghosts);
    writer.addEdgeProperty<types::String>(remyGhosts, "name", "Remy -> Ghosts");
    writer.addEdgeProperty<types::Int64>(remyGhosts, "duration", 20);
    writer.addEdgeProperty<types::String>(remyGhosts, "proficiency", "expert");

    auto remyComputers = writer.addEdge("INTERESTED_IN", remy, computers);
    writer.addEdgeProperty<types::String>(remyComputers, "name", "Remy -> Computers");
    writer.addEdgeProperty<types::String>(remyComputers, "proficiency", "expert");

    auto remyEighties = writer.addEdge("INTERESTED_IN", remy, eighties);
    writer.addEdgeProperty<types::String>(remyEighties, "name", "Remy -> Eighties");
    writer.addEdgeProperty<types::Int64>(remyEighties, "duration", 20);
    writer.addEdgeProperty<types::String>(remyEighties, "proficiency", "moderate");

    auto adamBio = writer.addEdge("INTERESTED_IN", adam, bio);
    writer.addEdgeProperty<types::String>(adamBio, "name", "Adam -> Bio");

    auto adamCooking = writer.addEdge("INTERESTED_IN", adam, cooking);
    writer.addEdgeProperty<types::String>(adamCooking, "name", "Adam -> Cooking");

    auto ghostsRemy = writer.addEdge("KNOWS_WELL", ghosts, remy);
    writer.addEdgeProperty<types::String>(ghostsRemy, "name", "Ghosts -> Remy");
    writer.addEdgeProperty<types::String>(ghostsRemy, "proficiency", "expert");
    writer.addEdgeProperty<types::Int64>(ghostsRemy, "duration", 200);

    writer.commit();

    auto maxime = writer.addNode({"Person", "Bioinformatics"});
    writer.addNodeProperty<types::String>(maxime, "name", "Maxime");
    writer.addNodeProperty<types::String>(maxime, "dob", "24/07");
    writer.addNodeProperty<types::Bool>(maxime, "isFrench", true);
    writer.addNodeProperty<types::Bool>(maxime, "hasPhD", false);

    auto paddle = writer.addNode({"Interest"});
    writer.addNodeProperty<types::String>(paddle, "name", "Paddle");

    auto maximeBio = writer.addEdge("INTERESTED_IN", maxime, bio);
    writer.addEdgeProperty<types::String>(maximeBio, "name", "Maxime -> Bio");

    auto maximePaddle = writer.addEdge("INTERESTED_IN", maxime, paddle);
    writer.addEdgeProperty<types::String>(maximePaddle, "name", "Maxime -> Paddle");
    writer.addEdgeProperty<types::String>(maximePaddle, "proficiency", "expert");

    writer.commit();

    auto luc = writer.addNode({"Person", "SoftwareEngineering"});
    writer.addNodeProperty<types::String>(luc, "dob", "28/05");
    writer.addNodeProperty<types::String>(luc, "name", "Luc");
    writer.addNodeProperty<types::Bool>(luc, "isFrench", true);
    writer.addNodeProperty<types::Bool>(luc, "hasPhD", true);

    auto animals = writer.addNode({"Interest", "SleepDisturber"});
    writer.addNodeProperty<types::String>(animals, "name", "Animals");
    writer.addNodeProperty<types::Bool>(animals, "isReal", true);

    auto lucAnimals = writer.addEdge("INTERESTED_IN", luc, animals);
    writer.addEdgeProperty<types::String>(lucAnimals, "name", "Luc -> Animals");
    writer.addEdgeProperty<types::Int64>(lucAnimals, "duration", 20);

    auto lucComputers = writer.addEdge("INTERESTED_IN", luc, computers);
    writer.addEdgeProperty<types::String>(lucComputers, "name", "Luc -> Computers");
    writer.addEdgeProperty<types::Int64>(lucComputers, "duration", 15);

    writer.commit();

    auto martina = writer.addNode({"Person", "Bioinformatics"});
    writer.addNodeProperty<types::String>(martina, "name", "Martina");
    writer.addNodeProperty<types::Bool>(martina, "isFrench", false);
    writer.addNodeProperty<types::Bool>(martina, "hasPhD", true);

    auto martinaCooking = writer.addEdge("INTERESTED_IN", martina, cooking);
    writer.addEdgeProperty<types::String>(martinaCooking, "name", "Martina -> Cooking");
    writer.addEdgeProperty<types::Int64>(martinaCooking, "duration", 10);

    writer.commit();

    auto suhas = writer.addNode({"Person", "SoftwareEngineering"});
    writer.addNodeProperty<types::String>(suhas, "name", "Suhas");
    writer.addNodeProperty<types::Bool>(suhas, "isFrench", false);
    writer.addNodeProperty<types::Bool>(suhas, "hasPhD", false);

    writer.commit();
}
