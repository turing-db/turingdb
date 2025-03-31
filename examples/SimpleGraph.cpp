#include "SimpleGraph.h"

#include "TuringDB.h"
#include "SystemManager.h"
#include "JobSystem.h"
#include "Graph.h"
#include "EntityID.h"
#include "types/PropertyType.h"
#include "writers/DataPartBuilder.h"

using namespace db;

void SimpleGraph::createSimpleGraph(TuringDB& db) {
    auto jobSystem = JobSystem::create();

    auto& system = db.getSystemManager();

    Graph* graph = system.getDefaultGraph();
    auto builder = graph->newPartWriter();
    auto* metadata = graph->getMetadata();
    auto& labels = metadata->labels();
    auto& labelsets = metadata->labelsets();
    auto& proptypes = metadata->propTypes();
    auto& edgetypes = metadata->edgeTypes();

    const auto getLabelSet = [&](std::initializer_list<std::string> labelList) {
        LabelSet lset;
        for (const auto& l : labelList) {
            lset.set(labels.getOrCreate(l));
        }
        return labelsets.getOrCreate(lset);
    };

    // EdgeTypes
    const EdgeTypeID knowsWellID = edgetypes.getOrCreate("KNOWS_WELL");
    const EdgeTypeID interestedInID = edgetypes.getOrCreate("INTERESTED_IN");

    // Persons
    const EntityID remy = builder->addNode(getLabelSet({"Person", "SoftwareEngineering", "Founder"}));
    const EntityID adam = builder->addNode(getLabelSet({"Person", "Bioinformatics", "Founder"}));
    const EntityID luc = builder->addNode(getLabelSet({"Person", "SoftwareEngineering"}));
    const EntityID maxime = builder->addNode(getLabelSet({"Person", "Bioinformatics"}));
    const EntityID martina = builder->addNode(getLabelSet({"Person", "Bioinformatics"}));
    const EntityID suhas = builder->addNode(getLabelSet({"Person", "SoftwareEngineering"}));

    // Interests
    const EntityID ghosts = builder->addNode(getLabelSet({"Interest", "Supernatural", "SleepDisturber", "Exotic"}));
    const EntityID bio = builder->addNode(getLabelSet({"Interest"}));
    const EntityID cooking = builder->addNode(getLabelSet({"Interest"}));
    const EntityID paddle = builder->addNode(getLabelSet({"Interest"}));
    const EntityID animals = builder->addNode(getLabelSet({"Interest", "SleepDisturber"}));
    const EntityID computers = builder->addNode(getLabelSet({"Interest", "SoftwareEngineering"}));
    const EntityID eighties = builder->addNode(getLabelSet({"Interest", "Exotic"}));

    // Property types
    const PropertyType name = proptypes.getOrCreate("name", types::String::_valueType);
    const PropertyType dob = proptypes.getOrCreate("dob", types::String::_valueType);
    const PropertyType isFrench = proptypes.getOrCreate("isFrench", types::Bool::_valueType);
    const PropertyType hasPhD = proptypes.getOrCreate("hasPhD", types::Bool::_valueType);
    const PropertyType isReal = proptypes.getOrCreate("isReal", types::Bool::_valueType);
    const PropertyType proficiency = proptypes.getOrCreate("proficiency", types::String::_valueType);
    const PropertyType duration = proptypes.getOrCreate("duration", types::Int64::_valueType);

    // Edges
    const EdgeRecord e01 = builder->addEdge(knowsWellID, remy, adam);
    const EdgeRecord e02 = builder->addEdge(knowsWellID, adam, remy);
    const EdgeRecord e03 = builder->addEdge(interestedInID, remy, ghosts);
    const EdgeRecord e04 = builder->addEdge(interestedInID, remy, computers);
    const EdgeRecord e05 = builder->addEdge(interestedInID, remy, eighties);
    const EdgeRecord e06 = builder->addEdge(interestedInID, adam, bio);
    const EdgeRecord e07 = builder->addEdge(interestedInID, adam, cooking);
    const EdgeRecord e08 = builder->addEdge(interestedInID, luc, animals);
    const EdgeRecord e09 = builder->addEdge(interestedInID, luc, computers);
    const EdgeRecord e10 = builder->addEdge(interestedInID, maxime, bio);
    const EdgeRecord e11 = builder->addEdge(interestedInID, maxime, paddle);
    const EdgeRecord e12 = builder->addEdge(interestedInID, martina, cooking);
    const EdgeRecord e13 = builder->addEdge(knowsWellID, ghosts, remy);

    // Node Properties

    // name:
    builder->addNodeProperty<types::String>(remy, name._id, "Remy");
    builder->addNodeProperty<types::String>(adam, name._id, "Adam");
    builder->addNodeProperty<types::String>(luc, name._id, "Luc");
    builder->addNodeProperty<types::String>(maxime, name._id, "Maxime");
    builder->addNodeProperty<types::String>(martina, name._id, "Martina");
    builder->addNodeProperty<types::String>(suhas, name._id, "Suhas");
    builder->addNodeProperty<types::String>(ghosts, name._id, "Ghosts");
    builder->addNodeProperty<types::String>(bio, name._id, "Bio");
    builder->addNodeProperty<types::String>(cooking, name._id, "Cooking");
    builder->addNodeProperty<types::String>(paddle, name._id, "Paddle");
    builder->addNodeProperty<types::String>(animals, name._id, "Animals");
    builder->addNodeProperty<types::String>(computers, name._id, "Computers");
    builder->addNodeProperty<types::String>(eighties, name._id, "Eighties");

    // dob:
    builder->addNodeProperty<types::String>(remy, dob._id, "18/01");
    builder->addNodeProperty<types::String>(adam, dob._id, "18/08");
    builder->addNodeProperty<types::String>(luc, dob._id, "28/05");
    builder->addNodeProperty<types::String>(maxime, dob._id, "24/07");
    builder->addNodeProperty<types::String>(suhas, dob._id, "12/09");

    // isFrench
    builder->addNodeProperty<types::Bool>(remy, isFrench._id, true);
    builder->addNodeProperty<types::Bool>(adam, isFrench._id, true);
    builder->addNodeProperty<types::Bool>(luc, isFrench._id, true);
    builder->addNodeProperty<types::Bool>(maxime, isFrench._id, true);
    builder->addNodeProperty<types::Bool>(martina, isFrench._id, false);
    builder->addNodeProperty<types::Bool>(suhas, isFrench._id, false);

    // hasPhD
    builder->addNodeProperty<types::Bool>(remy, hasPhD._id, true);
    builder->addNodeProperty<types::Bool>(adam, hasPhD._id, true);
    builder->addNodeProperty<types::Bool>(luc, hasPhD._id, true);
    builder->addNodeProperty<types::Bool>(maxime, hasPhD._id, false);
    builder->addNodeProperty<types::Bool>(martina, hasPhD._id, true);
    builder->addNodeProperty<types::Bool>(suhas, hasPhD._id, false);

    // isReal
    builder->addNodeProperty<types::Bool>(ghosts, isReal._id, true);
    builder->addNodeProperty<types::Bool>(computers, isReal._id, true);
    builder->addNodeProperty<types::Bool>(eighties, isReal._id, false);
    builder->addNodeProperty<types::Bool>(animals, isReal._id, true);

    // Edge Properties
    builder->addEdgeProperty<types::String>(e01, name._id, "Remy -> Adam");
    builder->addEdgeProperty<types::String>(e02, name._id, "Adam -> Remy");
    builder->addEdgeProperty<types::String>(e03, name._id, "Remy -> Ghosts");
    builder->addEdgeProperty<types::String>(e04, name._id, "Remy -> Computers");
    builder->addEdgeProperty<types::String>(e05, name._id, "Remy -> Eighties");
    builder->addEdgeProperty<types::String>(e06, name._id, "Adam -> Bio");
    builder->addEdgeProperty<types::String>(e07, name._id, "Adam -> Cooking");
    builder->addEdgeProperty<types::String>(e08, name._id, "Luc -> Animals");
    builder->addEdgeProperty<types::String>(e09, name._id, "Luc -> Computers");
    builder->addEdgeProperty<types::String>(e10, name._id, "Maxime -> Bio");
    builder->addEdgeProperty<types::String>(e11, name._id, "Maxime -> Paddle");
    builder->addEdgeProperty<types::String>(e12, name._id, "Martina -> Cooking");
    builder->addEdgeProperty<types::String>(e13, name._id, "Ghosts -> Remy");

    builder->addEdgeProperty<types::String>(e03, proficiency._id, "expert");
    builder->addEdgeProperty<types::String>(e04, proficiency._id, "expert");
    builder->addEdgeProperty<types::String>(e05, proficiency._id, "moderate");
    builder->addEdgeProperty<types::String>(e11, proficiency._id, "expert");
    builder->addEdgeProperty<types::String>(e13, proficiency._id, "expert");

    builder->addEdgeProperty<types::Int64>(e01, duration._id, 20);
    builder->addEdgeProperty<types::Int64>(e02, duration._id, 20);
    builder->addEdgeProperty<types::Int64>(e03, duration._id, 20);
    builder->addEdgeProperty<types::Int64>(e05, duration._id, 20);
    builder->addEdgeProperty<types::Int64>(e08, duration._id, 20);
    builder->addEdgeProperty<types::Int64>(e09, duration._id, 15);
    builder->addEdgeProperty<types::Int64>(e12, duration._id, 10);
    builder->addEdgeProperty<types::Int64>(e13, duration._id, 200);

    builder->commit(*jobSystem);
    jobSystem->terminate();
}
