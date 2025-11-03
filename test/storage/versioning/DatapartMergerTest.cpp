#include "gtest/gtest.h"

#include "TuringTest.h"

#include "mergers/DataPartMerger.h"
#include "writers/DataPartBuilder.h"
#include "metadata/GraphMetadata.h"
#include "EdgeContainer.h"
#include "NodeContainer.h"
#include "versioning/CommitBuilder.h"
#include "writers/MetadataBuilder.h"
#include "Graph.h"
#include "writers/GraphWriter.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"

#include "JobSystem.h"

using namespace turing::test;
using namespace ::testing;
using namespace db;

// Helper Class To Map Old EntityIDs to New EntityIDs
template <typename EntityType>
class EntityMap {
public:
    struct EntityMapping {
        int64_t _propertyID {};
        EntityType _oldID;
        EntityType _newID;

        EntityMapping() = delete;

        explicit EntityMapping(int64_t propertyID)
            : _propertyID(propertyID)
        {
        }
    };

    using EntityMappingContainer = std::vector<EntityMapping>;
    using propertyIDMap = std::unordered_map<int64_t, size_t>;
    using oldEntityMap = std::unordered_map<EntityType, size_t>;
    using newEntityMap = std::unordered_map<EntityType, size_t>;

    void createMapping(int64_t propertyID, EntityType oldID) {
        auto& entry = _container.emplace_back(propertyID);
        entry._oldID = oldID;
        _propertyMap.emplace(propertyID, _container.size() - 1);
        _oldMap.emplace(oldID, _container.size() - 1);
    }

    void addNewId(int64_t propertyID, EntityType newID) {
        const auto& it = _propertyMap.find(propertyID);
        if (it == _propertyMap.end()) {
            throw std::runtime_error(fmt::format("Could not find property"));
        }
        _container[it->second]._newID = newID;
        _newMap.emplace(newID, it->second);
    }

    const EntityType& getOld(EntityType newID) {
        const auto& it = _newMap.find(newID);
        if (it == _newMap.end()) {
            throw std::runtime_error(fmt::format("Could not find new entity"));
        }
        return _container[it->second]._oldID;
    }

    const EntityType& getNew(EntityType oldID) {
        const auto& it = _oldMap.find(oldID);
        if (it == _oldMap.end()) {
            throw std::runtime_error(fmt::format("Could not find old entity"));
        }
        return _container[it->second]._newID;
    }

    int64_t getPropertyFromNew(EntityType newID) {
        const auto& it = _newMap.find(newID);
        if (it == _newMap.end()) {
            throw std::runtime_error(fmt::format("Could not find new entity"));
        }
        return _container[it->second]._propertyID;
    }

    int64_t getPropertyFromOld(EntityType oldID) {
        const auto& it = _oldMap.find(oldID);
        if (it == _oldMap.end()) {
            throw std::runtime_error(fmt::format("Could not find old entity"));
        }
        return _container[it->second]._propertyID;
    }

    const EntityType& getOldFromProperty(int64_t propertyID) {
        const auto& it = _propertyMap.find(propertyID);
        if (it == _propertyMap.end()) {
            throw std::runtime_error(fmt::format("Could not find property"));
        }
        return _container[it->second]._oldID;
    }

    const EntityType& getNewFromProperty(int64_t propertyID) {
        const auto& it = _propertyMap.find(propertyID);
        if (it == _propertyMap.end()) {
            throw std::runtime_error(fmt::format("Could not find property"));
        }
        return _container[it->second]._newID;
    }

private:
    EntityMappingContainer _container;
    propertyIDMap _propertyMap;
    oldEntityMap _oldMap;
    newEntityMap _newMap;
};

using PropertyMap = std::variant<
    std::unordered_map<int64_t, types::Double::Primitive>,
    std::unordered_map<int64_t, types::UInt64::Primitive>,
    std::unordered_map<int64_t, types::Int64::Primitive>,
    std::unordered_map<int64_t, types::String::Primitive>,
    std::unordered_map<int64_t, types::Bool::Primitive>>;

class DataPartMergerTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = JobSystem::create();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    EdgeRecord addEdgeHelper(GraphWriter& writer,
                             std::string_view edgeType,
                             NodeID sourceNodeID,
                             NodeID otherNodeID,
                             int64_t edgePropID,
                             const std::unordered_map<NodeID, int64_t>& tmpNodeIDToPropMap) {
        EdgeRecord record = writer.addEdge(edgeType, sourceNodeID, otherNodeID);

        _nodeToEdgeMap[tmpNodeIDToPropMap.at(sourceNodeID)].emplace(edgePropID);
        _nodeToEdgeMap[tmpNodeIDToPropMap.at(otherNodeID)].emplace(edgePropID);

        return record;
    }

    template <SupportedType T>
    void addNodePropertyHelper(GraphWriter& writer,
                               NodeID nodeID,
                               std::string_view propName,
                               typename T::Primitive&& value,
                               int64_t id) {
        typename T::Primitive valueCopy = value;

        writer.addNodeProperty<T>(nodeID, propName, std::move(value));

        const auto propID = *writer.openWriteTransaction().readGraph().getMetadata().propTypes().get(propName);
        const auto& it = _nodePropertyMap.find(propID._id.getValue());
        if (it == _nodePropertyMap.end()) {
            if constexpr (std::is_same_v<T, types::Int64>) {
                _nodePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::Int64::Primitive> {});
            } else if constexpr (std::is_same_v<T, types::String>) {
                _nodePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::String::Primitive> {});
            } else if constexpr (std::is_same_v<T, types::Bool>) {
                _nodePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::Bool::Primitive> {});
            } else if constexpr (std::is_same_v<T, types::UInt64>) {
                _nodePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::UInt64::Primitive> {});
            } else if constexpr (std::is_same_v<T, types::Double>) {
                _nodePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::Double::Primitive> {});
            }
        }

        _nodeToPropertyIDMap[id].emplace(propID._id.getValue());
        std::visit([&](auto& map) {
            using MapValueType = typename std::decay_t<decltype(map)>::mapped_type;
            if constexpr (std::is_same_v<MapValueType, typename T::Primitive>) {
                map[id] = valueCopy;
            }
        }, _nodePropertyMap.at(propID._id.getValue()));
    }

    template <SupportedType T>
    void addEdgePropertyHelper(GraphWriter& writer,
                               const EdgeRecord& edge,
                               std::string_view propName,
                               typename T::Primitive&& value,
                               int64_t id) {
        typename T::Primitive valueCopy = value;

        writer.addEdgeProperty<T>(edge, propName, std::move(value));

        const auto propID = *writer.openWriteTransaction().readGraph().getMetadata().propTypes().get(propName);
        const auto& it = _edgePropertyMap.find(propID._id.getValue());
        if (it == _edgePropertyMap.end()) {
            if constexpr (std::is_same_v<T, types::Int64>) {
                _edgePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::Int64::Primitive> {});
            } else if constexpr (std::is_same_v<T, types::String>) {
                _edgePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::String::Primitive> {});
            } else if constexpr (std::is_same_v<T, types::Bool>) {
                _edgePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::Bool::Primitive> {});
            } else if constexpr (std::is_same_v<T, types::UInt64>) {
                _edgePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::UInt64::Primitive> {});
            } else if constexpr (std::is_same_v<T, types::Double>) {
                _edgePropertyMap.emplace(propID._id.getValue(), std::unordered_map<int64_t, types::Double::Primitive> {});
            }
        }

        _edgeToPropertyIDMap[id].emplace(propID._id.getValue());
        std::visit([&](auto& map) {
            using MapValueType = typename std::decay_t<decltype(map)>::mapped_type;
            if constexpr (std::is_same_v<MapValueType, typename T::Primitive>) {
                map[id] = valueCopy;
            }
        }, _edgePropertyMap.at(propID._id.getValue()));
    }

    template <SupportedType T>
    NodeID findNodeIDInWriter(GraphWriter& writer,
                              T::Primitive value,
                              PropertyTypeID propTypeID) {
        const auto transaction = writer.openWriteTransaction();
        const auto reader = transaction.readGraph();

        auto it = reader.scanNodeProperties<T>(propTypeID).begin();
        for (; it.isValid(); it.next()) {
            if (it.get() == value) {
                return it.getCurrentNodeID();
            }
        }
        throw std::runtime_error(fmt::format("Node not found: {}", value));
    };

    template <SupportedType T>
    EdgeID findEdgeIDInWriter(GraphWriter& writer,
                              T::Primitive value,
                              PropertyTypeID propTypeID) {
        const auto transaction = writer.openWriteTransaction();
        const auto reader = transaction.readGraph();

        auto it = reader.scanEdgeProperties<T>(propTypeID).begin();
        for (; it.isValid(); it.next()) {
            if (it.get() == value) {
                return it.getCurrentEdgeID();
            }
        }
        throw std::runtime_error(fmt::format("edge not found: {}", value));
    };

    void createBasicGraph(GraphWriter& writer) {
        const auto idPropertyType = writer.addPropertyType("id", ValueType::Int64);
        const auto namePropertyType = writer.addPropertyType("name", ValueType::String);

        // Map the pre-sorted nodeIDs to the id property value
        // We need this to map the property id for edges to nodes
        std::unordered_map<NodeID, int64_t> tmpNodeIDToIdPropMap;
        // Reset the map after writer.commit() when the values get sorted
        const auto resetTmpNodeIDToIdPropMap = [&]() {
            const auto transaction = writer.openWriteTransaction();
            const auto reader = transaction.readGraph();

            auto it = reader.scanNodeProperties<types::Int64>(idPropertyType._id).begin();
            for (; it.isValid(); it.next()) {
                tmpNodeIDToIdPropMap[it.getCurrentNodeID()] = it.get();
            }
        };

        const auto remy = writer.addNode({"Person", "SoftwareEngineering", "Founder"});
        addNodePropertyHelper<types::String>(writer, remy, "name", "Remy", 1);
        addNodePropertyHelper<types::String>(writer, remy, "dob", "18/01", 1);
        addNodePropertyHelper<types::Int64>(writer, remy, "age", 32, 1);
        addNodePropertyHelper<types::Bool>(writer, remy, "isFrench", true, 1);
        addNodePropertyHelper<types::Bool>(writer, remy, "hasPhD", true, 1);
        writer.addNodeProperty<types::Int64>(remy, "id", 1);
        tmpNodeIDToIdPropMap.emplace(remy, 1);

        const auto adam = writer.addNode({"Person", "Bioinformatics", "Founder"});
        addNodePropertyHelper<types::String>(writer, adam, "name", "Adam", 2);
        addNodePropertyHelper<types::String>(writer, adam, "dob", "28/08", 2);
        addNodePropertyHelper<types::Int64>(writer, adam, "age", 32, 2);
        addNodePropertyHelper<types::Bool>(writer, adam, "isFrench", true, 2);
        addNodePropertyHelper<types::Bool>(writer, adam, "hasPhD", true, 2);
        writer.addNodeProperty<types::Int64>(adam, "id", 2);
        tmpNodeIDToIdPropMap.emplace(adam, 2);

        const auto computers = writer.addNode({"Interest", "SoftwareEngineering"});
        addNodePropertyHelper<types::String>(writer, computers, "name", "Computers", 3);
        addNodePropertyHelper<types::Bool>(writer, computers, "isReal", true, 3);
        writer.addNodeProperty<types::Int64>(computers, "id", 3);
        tmpNodeIDToIdPropMap.emplace(computers, 3);

        const auto eighties = writer.addNode({"Interest", "Exotic"});
        addNodePropertyHelper<types::String>(writer, eighties, "name", "Eighties", 4);
        addNodePropertyHelper<types::Bool>(writer, eighties, "isReal", false, 4);
        writer.addNodeProperty<types::Int64>(eighties, "id", 4);
        tmpNodeIDToIdPropMap.emplace(eighties, 4);

        const auto bio = writer.addNode({"Interest"});
        addNodePropertyHelper<types::String>(writer, bio, "name", "Bio", 5);
        writer.addNodeProperty<types::Int64>(bio, "id", 5);
        tmpNodeIDToIdPropMap.emplace(bio, 5);

        const auto cooking = writer.addNode({"Interest"});
        addNodePropertyHelper<types::String>(writer, cooking, "name", "Cooking", 6);
        writer.addNodeProperty<types::Int64>(cooking, "id", 6);
        tmpNodeIDToIdPropMap.emplace(cooking, 6);

        const auto ghosts = writer.addNode({"Interest", "Supernatural", "SleepDisturber", "Exotic"}); // 6
        addNodePropertyHelper<types::String>(writer, ghosts, "name", "Ghosts", 7);
        addNodePropertyHelper<types::Bool>(writer, ghosts, "isReal", true, 7);
        writer.addNodeProperty<types::Int64>(ghosts, "id", 7);
        tmpNodeIDToIdPropMap.emplace(ghosts, 7);

        const auto remyAdam = addEdgeHelper(writer, "KNOWS_WELL", remy, adam, 1, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, remyAdam, "name", "Remy -> Adam", 1);
        addEdgePropertyHelper<types::Int64>(writer, remyAdam, "duration", 20, 1);
        writer.addEdgeProperty<types::Int64>(remyAdam, "id", 1);

        const auto adamRemy = addEdgeHelper(writer, "KNOWS_WELL", adam, remy, 2, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, adamRemy, "name", "Adam -> Remy", 2);
        addEdgePropertyHelper<types::Int64>(writer, adamRemy, "duration", 20, 2);
        writer.addEdgeProperty<types::Int64>(adamRemy, "id", 2);

        const auto remyGhosts = addEdgeHelper(writer, "INTERESTED_IN", remy, ghosts, 3, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, remyGhosts, "name", "Remy -> Ghosts", 3);
        addEdgePropertyHelper<types::Int64>(writer, remyGhosts, "duration", 20, 3);
        addEdgePropertyHelper<types::String>(writer, remyGhosts, "proficiency", "expert", 3);
        writer.addEdgeProperty<types::Int64>(remyGhosts, "id", 3);

        const auto remyComputers = addEdgeHelper(writer, "INTERESTED_IN", remy, computers, 4, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, remyComputers, "name", "Remy -> Computers", 4);
        addEdgePropertyHelper<types::String>(writer, remyComputers, "proficiency", "expert", 4);
        writer.addEdgeProperty<types::Int64>(remyComputers, "id", 4);

        const auto remyEighties = addEdgeHelper(writer, "INTERESTED_IN", remy, eighties, 5, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, remyEighties, "name", "Remy -> Eighties", 5);
        addEdgePropertyHelper<types::Int64>(writer, remyEighties, "duration", 20, 5);
        addEdgePropertyHelper<types::String>(writer, remyEighties, "proficiency", "moderate", 5);
        writer.addEdgeProperty<types::Int64>(remyEighties, "id", 5);

        const auto adamBio = addEdgeHelper(writer, "INTERESTED_IN", adam, bio, 6, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, adamBio, "name", "Adam -> Bio", 6);
        writer.addEdgeProperty<types::Int64>(adamBio, "id", 6);

        const auto adamCooking = addEdgeHelper(writer, "INTERESTED_IN", adam, cooking, 7, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, adamCooking, "name", "Adam -> Cooking", 7);
        writer.addEdgeProperty<types::Int64>(adamCooking, "id", 7);

        const auto ghostsRemy = addEdgeHelper(writer, "KNOWS_WELL", ghosts, remy, 8, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, ghostsRemy, "name", "Ghosts -> Remy", 8);
        addEdgePropertyHelper<types::String>(writer, ghostsRemy, "proficiency", "expert", 8);
        addEdgePropertyHelper<types::Int64>(writer, ghostsRemy, "duration", 200, 8);
        writer.addEdgeProperty<types::Int64>(ghostsRemy, "id", 8);

        writer.commit();
        resetTmpNodeIDToIdPropMap();

        const auto maxime = writer.addNode({"Person", "Bioinformatics"});
        addNodePropertyHelper<types::String>(writer, maxime, "name", "Maxime", 8);
        addNodePropertyHelper<types::String>(writer, maxime, "dob", "24/07", 8);
        addNodePropertyHelper<types::Bool>(writer, maxime, "isFrench", true, 8);
        addNodePropertyHelper<types::Bool>(writer, maxime, "hasPhD", false, 8);
        writer.addNodeProperty<types::Int64>(maxime, "id", 8);
        tmpNodeIDToIdPropMap.emplace(maxime, 8);

        const auto paddle = writer.addNode({"Interest"});
        addNodePropertyHelper<types::String>(writer, paddle, "name", "Paddle", 9);
        writer.addNodeProperty<types::Int64>(paddle, "id", 9);
        tmpNodeIDToIdPropMap.emplace(paddle, 9);

        const auto maximeBio = addEdgeHelper(writer, "INTERESTED_IN",
                                             maxime,
                                             findNodeIDInWriter<types::String>(writer, "Bio", namePropertyType._id),
                                             9,
                                             tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, maximeBio, "name", "Maxime -> Bio", 9);
        writer.addEdgeProperty<types::Int64>(maximeBio, "id", 9);

        const auto maximePaddle = addEdgeHelper(writer, "INTERESTED_IN", maxime, paddle, 10, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, maximePaddle, "name", "Maxime -> Paddle", 10);
        addEdgePropertyHelper<types::String>(writer, maximePaddle, "proficiency", "expert", 10);
        writer.addEdgeProperty<types::Int64>(maximePaddle, "id", 10);

        writer.commit();
        resetTmpNodeIDToIdPropMap();

        const auto luc = writer.addNode({"Person", "SoftwareEngineering"});
        addNodePropertyHelper<types::String>(writer, luc, "name", "Luc", 10);
        addNodePropertyHelper<types::String>(writer, luc, "dob", "28/05", 10);
        addNodePropertyHelper<types::Bool>(writer, luc, "isFrench", true, 10);
        addNodePropertyHelper<types::Bool>(writer, luc, "hasPhD", true, 10);
        writer.addNodeProperty<types::Int64>(luc, "id", 10);
        tmpNodeIDToIdPropMap.emplace(luc, 10);

        const auto animals = writer.addNode({"Interest", "SleepDisturber"});
        addNodePropertyHelper<types::String>(writer, animals, "name", "Animals", 11);
        addNodePropertyHelper<types::Bool>(writer, animals, "isReal", true, 11);
        writer.addNodeProperty<types::Int64>(animals, "id", 11);
        tmpNodeIDToIdPropMap.emplace(animals, 11);

        const auto lucAnimals = addEdgeHelper(writer, "INTERESTED_IN", luc, animals, 11, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, lucAnimals, "name", "Luc -> Animals", 11);
        addEdgePropertyHelper<types::Int64>(writer, lucAnimals, "duration", 20, 11);
        writer.addEdgeProperty<types::Int64>(lucAnimals, "id", 11);

        const auto lucComputers = addEdgeHelper(writer,
                                                "INTERESTED_IN",
                                                luc,
                                                findNodeIDInWriter<types::String>(writer, "Computers", namePropertyType._id),
                                                12, tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, lucComputers, "name", "Luc -> Computers", 12);
        addEdgePropertyHelper<types::Int64>(writer, lucComputers, "duration", 15, 12);
        writer.addEdgeProperty<types::Int64>(lucComputers, "id", 12);

        writer.commit();
        resetTmpNodeIDToIdPropMap();

        const auto martina = writer.addNode({"Person", "Bioinformatics"});
        addNodePropertyHelper<types::String>(writer, martina, "name", "Martina", 12);
        addNodePropertyHelper<types::Bool>(writer, martina, "isFrench", false, 12);
        addNodePropertyHelper<types::Bool>(writer, martina, "hasPhD", true, 12);
        writer.addNodeProperty<types::Int64>(martina, "id", 12);
        tmpNodeIDToIdPropMap.emplace(martina, 12);

        const auto martinaCooking = addEdgeHelper(writer,
                                                  "INTERESTED_IN",
                                                  martina,
                                                  findNodeIDInWriter<types::String>(writer, "Cooking", namePropertyType._id),
                                                  13,
                                                  tmpNodeIDToIdPropMap);
        addEdgePropertyHelper<types::String>(writer, martinaCooking, "name", "Martina -> Cooking", 13);
        addEdgePropertyHelper<types::Int64>(writer, martinaCooking, "duration", 10, 13);
        writer.addEdgeProperty<types::Int64>(martinaCooking, "id", 13);

        writer.commit();
        resetTmpNodeIDToIdPropMap();

        const auto suhas = writer.addNode({"Person", "SoftwareEngineering"});
        addNodePropertyHelper<types::String>(writer, suhas, "name", "Suhas", 13);
        addNodePropertyHelper<types::Bool>(writer, suhas, "isFrench", false, 13);
        addNodePropertyHelper<types::Bool>(writer, suhas, "hasPhD", false, 13);
        writer.addNodeProperty<types::Int64>(suhas, "id", 13);
        tmpNodeIDToIdPropMap.emplace(suhas, 13);

        writer.commit();
        resetTmpNodeIDToIdPropMap();

        const auto cyrus = writer.addNode({"Person", "SoftwareEngineering"});
        addNodePropertyHelper<types::String>(writer, cyrus, "name", "Cyrus", 14);
        addNodePropertyHelper<types::Bool>(writer, cyrus, "isFrench", false, 14);
        addNodePropertyHelper<types::Bool>(writer, cyrus, "hasPhD", false, 14);
        writer.addNodeProperty<types::Int64>(cyrus, "id", 14);
        tmpNodeIDToIdPropMap.emplace(cyrus, 15);

        writer.submit();
    }

    std::unique_ptr<db::JobSystem> _jobSystem;

    // The two maps below help us map between
    // preMergingIDs<-id property type->postMergingIDs
    EntityMap<EdgeID> _edgeMap;
    EntityMap<NodeID> _nodeMap;

    // The two maps below help us map between
    // PropertyTypeID->node/edge id property type-> property values
    // it lets us confirm whether entities have the correct properties
    std::unordered_map<uint16_t, PropertyMap> _nodePropertyMap;
    std::unordered_map<uint16_t, PropertyMap> _edgePropertyMap;

    // The _nodeToEdgeMap maps a node to all it's connected edges
    // This is useful to keep track of the hanging edges during deletion
    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> _nodeToEdgeMap;

    // The two maps below allow us to keep track of what property types
    // have been added to an entity, so on deletion we can confirm that
    // these property types do not exist
    std::unordered_map<int64_t, std::unordered_set<uint16_t>> _nodeToPropertyIDMap;
    std::unordered_map<int64_t, std::unordered_set<uint16_t>> _edgeToPropertyIDMap;
};

TEST_F(DataPartMergerTest, MergeTest) {
    auto graph = Graph::create();
    GraphWriter writer {graph.get()};

    createBasicGraph(writer);

    auto* commitBuilder = writer.openWriteTransaction().commitBuilder();

    DataPartMerger merger(&commitBuilder->commitData(), commitBuilder->metadata());
    std::unique_ptr<DataPartBuilder> dataPartBuilder = merger.merge(commitBuilder->commitData().allDataparts());

    DataPart mergedDataPart = DataPart(0, 0);

    mergedDataPart.load(commitBuilder->viewGraph(), *_jobSystem, *dataPartBuilder);

    const auto transaction = writer.openWriteTransaction();
    const auto reader = transaction.readGraph();

    const auto propTypeID = commitBuilder->metadata().getOrCreatePropertyType("id", ValueType::Int64);

    auto nodeIt = reader.scanNodeProperties<types::Int64>(propTypeID._id).begin();
    for (; nodeIt.isValid(); nodeIt.next()) {
        _nodeMap.createMapping(nodeIt.get(), nodeIt.getCurrentNodeID());
    }

    auto edgeIt = reader.scanEdgeProperties<types::Int64>(propTypeID._id).begin();
    for (; edgeIt.isValid(); edgeIt.next()) {
        _edgeMap.createMapping(edgeIt.get(), edgeIt.getCurrentEdgeID());
    }

    const auto& nodeIdContainer = mergedDataPart.nodeProperties().getContainer<types::Int64>(propTypeID._id);
    const auto& edgeIdContainer = mergedDataPart.edgeProperties().getContainer<types::Int64>(propTypeID._id);

    for (const auto& it : nodeIdContainer.zipped()) {
        _nodeMap.addNewId(it.second, static_cast<db::NodeID>(it.first.getValue()));
    }

    for (const auto& it : edgeIdContainer.zipped()) {
        _edgeMap.addNewId(it.second, static_cast<db::EdgeID>(it.first.getValue()));
    }

    ASSERT_EQ(mergedDataPart.nodes().size(), reader.getTotalNodesAllocated());
    ASSERT_EQ(mergedDataPart.edgeIndexer().getCoreNodeCount(), mergedDataPart.nodes().size());

    ASSERT_EQ(mergedDataPart.edges().size(), reader.getTotalEdgesAllocated());

    ASSERT_EQ(mergedDataPart.edgeIndexer().getPatchNodeCount(), 0);

    for (const auto& it : mergedDataPart.edges().getOuts()) {
        const auto& oldEdge = reader.getEdge(_edgeMap.getOld(it._edgeID));
        ASSERT_EQ(_nodeMap.getNew(oldEdge->_nodeID), it._nodeID);
        ASSERT_EQ(_nodeMap.getNew(oldEdge->_otherID), it._otherID);
    }

    for (const auto& it : mergedDataPart.nodeProperties()) {
        // skip comparing the 'id' property
        if (it.first == propTypeID._id) {
            continue;
        }

        switch (it.second->getValueType()) {
            case db::ValueType::Bool:
                for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::Bool>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::Bool::Primitive>>(_nodePropertyMap[it.first.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            case db::ValueType::Int64:
                for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::Int64>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::Int64::Primitive>>(_nodePropertyMap[it.first.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            case db::ValueType::String:
                for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::String>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::String::Primitive>>(_nodePropertyMap[it.first.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            case db::ValueType::UInt64:
                for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::UInt64>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::UInt64::Primitive>>(_nodePropertyMap[it.first.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            case db::ValueType::Double:
                for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::Double>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::Double::Primitive>>(_nodePropertyMap[it.first.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            default:
                throw std::runtime_error(fmt::format("Invalid Value Type"));
                break;
        }
    }

    for (const auto& it : mergedDataPart.edgeProperties()) {
        // skip comparing the 'id' property
        if (it.first == propTypeID._id) {
            continue;
        }

        switch (it.second->getValueType()) {
            case db::ValueType::Bool:
                for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::Bool>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::Bool::Primitive>>(_edgePropertyMap[it.first.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            case db::ValueType::Int64:
                for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::Int64>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::Int64::Primitive>>(_edgePropertyMap[it.first.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            case db::ValueType::String:
                for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::String>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::String::Primitive>>(_edgePropertyMap[it.first.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            case db::ValueType::UInt64:
                for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::UInt64>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::UInt64::Primitive>>(_edgePropertyMap[it.first.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            case db::ValueType::Double:
                for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::Double>(it.first).zipped()) {
                    const auto& oldVal = std::get<std::unordered_map<int64_t, types::Double::Primitive>>(_edgePropertyMap[it.first.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                    ASSERT_EQ(val, oldVal);
                }
                break;
            default:
                throw std::runtime_error(fmt::format("Invalid Value Type"));
                break;
        }
    }
}

TEST_F(DataPartMergerTest, DeleteNodeAndMerge) {
    auto graph = Graph::create();
    GraphWriter writer {graph.get()};

    createBasicGraph(writer);

    const int64_t deletedId = 1;
    const auto& hangingEdges = _nodeToEdgeMap[deletedId];

    const auto idPropType = graph->openTransaction().viewGraph().metadata().propTypes().get("id");

    writer.deleteNode(findNodeIDInWriter<types::Int64>(writer, deletedId, idPropType->_id));
    writer.submit();

    auto* commitBuilder = writer.openWriteTransaction().commitBuilder();
    DataPartMerger merger(&commitBuilder->commitData(), commitBuilder->metadata());
    std::unique_ptr<DataPartBuilder> dataPartBuilder = merger.merge(commitBuilder->commitData().allDataparts());

    DataPart mergedDataPart = DataPart(0, 0);
    mergedDataPart.load(commitBuilder->viewGraph(), *_jobSystem, *dataPartBuilder);

    // confirm that the deleted node and hanging edges exist in the unmerged graph
    ASSERT_NO_THROW(findNodeIDInWriter<types::Int64>(writer, deletedId, idPropType->_id));
    for (const auto& edge : hangingEdges) {
        ASSERT_NO_THROW(findEdgeIDInWriter<types::Int64>(writer, edge, idPropType->_id));
    }

    const auto transaction = writer.openWriteTransaction();
    const auto reader = transaction.readGraph();

    ASSERT_EQ(mergedDataPart.nodes().size(), reader.getTotalNodesAllocated() - 1);
    ASSERT_EQ(mergedDataPart.edges().size(), reader.getTotalEdgesAllocated() - hangingEdges.size());

    auto nodeIt = reader.scanNodeProperties<types::Int64>(idPropType->_id).begin();
    for (; nodeIt.isValid(); nodeIt.next()) {
        _nodeMap.createMapping(nodeIt.get(), nodeIt.getCurrentNodeID());
    }

    for (const auto& ptID : _nodeToPropertyIDMap[deletedId]) {
        // confirm that the all the deleted properties still exist in the unmerged node containers
        ASSERT_TRUE(reader.nodeHasProperty(ptID, _nodeMap.getOldFromProperty(deletedId)));
    }

    const auto& nodeIdContainer = mergedDataPart.nodeProperties().getContainer<types::Int64>(idPropType->_id);
    const auto& edgeIdContainer = mergedDataPart.edgeProperties().getContainer<types::Int64>(idPropType->_id);

    // Confirm that the node has been deleted
    for (const auto& [id, value] : nodeIdContainer.zipped()) {
        ASSERT_NE(value, deletedId);
        _nodeMap.addNewId(value, static_cast<db::NodeID>(id.getValue()));
    }

    // Confirm that all the hanging edges were deleted
    for (const auto& [id, value] : edgeIdContainer.zipped()) {
        ASSERT_FALSE(hangingEdges.contains(value));
    }

    // Loop through and find all the property containers affected by the deleted node
    for (const auto& [ptId, container] : mergedDataPart.nodeProperties()) {
        if (ptId == idPropType->_id) {
            continue;
        }

        if (_nodeToPropertyIDMap[deletedId].contains(ptId.getValue())) {
            // confirm that the property container has reduced in size by 1
            ASSERT_EQ(container->ids().size(), reader.getNodePropertyCount(ptId) - 1);

            // confirm that every other value is valid
            switch (container->getValueType()) {
                case db::ValueType::Bool:
                    for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::Bool>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::Bool::Primitive>>(_nodePropertyMap[ptId.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                case db::ValueType::Int64:
                    for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::Int64>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::Int64::Primitive>>(_nodePropertyMap[ptId.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                case db::ValueType::String:
                    for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::String>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::String::Primitive>>(_nodePropertyMap[ptId.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                case db::ValueType::UInt64:
                    for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::UInt64>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::UInt64::Primitive>>(_nodePropertyMap[ptId.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                case db::ValueType::Double:
                    for (const auto& [id, val] : mergedDataPart.nodeProperties().getContainer<types::Double>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::Double::Primitive>>(_nodePropertyMap[ptId.getValue()])[_nodeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                default:
                    throw std::runtime_error(fmt::format("Invalid Value Type"));
                    break;
            }
        }
    }
}

TEST_F(DataPartMergerTest, DeleteEdgeAndMerge) {
    auto graph = Graph::create();
    GraphWriter writer {graph.get()};

    createBasicGraph(writer);

    const int64_t deletedId = 1;
    const auto idPropType = graph->openTransaction().viewGraph().metadata().propTypes().get("id");

    writer.deleteEdge(findEdgeIDInWriter<types::Int64>(writer, deletedId, idPropType->_id));
    writer.submit();

    auto* commitBuilder = writer.openWriteTransaction().commitBuilder();
    DataPartMerger merger(&commitBuilder->commitData(), commitBuilder->metadata());
    std::unique_ptr<DataPartBuilder> dataPartBuilder = merger.merge(commitBuilder->commitData().allDataparts());

    DataPart mergedDataPart = DataPart(0, 0);
    mergedDataPart.load(commitBuilder->viewGraph(), *_jobSystem, *dataPartBuilder);

    // Confirm that the edge is present in the unmerged graph
    ASSERT_NO_THROW(findEdgeIDInWriter<types::Int64>(writer, deletedId, idPropType->_id));

    const auto transaction = writer.openWriteTransaction();
    const auto reader = transaction.readGraph();

    // Confirm that the edge container size has reduced by one
    ASSERT_EQ(mergedDataPart.edges().size(), reader.getTotalEdgesAllocated() - 1);

    auto edgeIt = reader.scanEdgeProperties<types::Int64>(idPropType->_id).begin();
    for (; edgeIt.isValid(); edgeIt.next()) {
        _edgeMap.createMapping(edgeIt.get(), edgeIt.getCurrentEdgeID());
    }

    const auto& edgeIdContainer = mergedDataPart.edgeProperties().getContainer<types::Int64>(idPropType->_id);
    for (const auto& [id, value] : edgeIdContainer.zipped()) {
        // Confirm that the deleted id prop doesn't exist
        ASSERT_NE(value, deletedId);
        _edgeMap.addNewId(value, static_cast<db::EdgeID>(id.getValue()));
    }

    for (const auto& [ptId, container] : mergedDataPart.edgeProperties()) {
        if (ptId == idPropType->_id) {
            continue;
        }
        // confirm that the property container has reduced in size by 1
        if (_edgeToPropertyIDMap[deletedId].contains(ptId.getValue())) {
            ASSERT_EQ(container->ids().size(), reader.getEdgePropertyCount(ptId) - 1);
            // confirm that every other value is the same as before
            switch (container->getValueType()) {
                case db::ValueType::Bool:
                    for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::Bool>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::Bool::Primitive>>(_edgePropertyMap[ptId.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                case db::ValueType::Int64:
                    for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::Int64>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::Int64::Primitive>>(_edgePropertyMap[ptId.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                case db::ValueType::String:
                    for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::String>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::String::Primitive>>(_edgePropertyMap[ptId.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                case db::ValueType::UInt64:
                    for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::UInt64>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::UInt64::Primitive>>(_edgePropertyMap[ptId.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                case db::ValueType::Double:
                    for (const auto& [id, val] : mergedDataPart.edgeProperties().getContainer<types::Double>(ptId).zipped()) {
                        const auto& oldVal = std::get<std::unordered_map<int64_t, types::Double::Primitive>>(_edgePropertyMap[ptId.getValue()])[_edgeMap.getPropertyFromNew(id.getValue())];
                        ASSERT_EQ(val, oldVal);
                    }
                    break;
                default:
                    throw std::runtime_error(fmt::format("Invalid Value Type"));
                    break;
            }
        }
    }
}

TEST_F(DataPartMergerTest, DeleteAllNodePropertiesAndMerge) {
    auto graph = Graph::create();
    GraphWriter writer {graph.get()};

    createBasicGraph(writer);

    const auto namePropType = graph->openTransaction().viewGraph().metadata().propTypes().get("name");
    const auto isRealPropType = graph->openTransaction().viewGraph().metadata().propTypes().get("isReal");

    writer.deleteNode(findNodeIDInWriter<types::String>(writer, "Computers", namePropType->_id));
    writer.deleteNode(findNodeIDInWriter<types::String>(writer, "Animals", namePropType->_id));
    writer.deleteNode(findNodeIDInWriter<types::String>(writer, "Eighties", namePropType->_id));
    writer.deleteNode(findNodeIDInWriter<types::String>(writer, "Ghosts", namePropType->_id));
    writer.submit();

    auto* commitBuilder = writer.openWriteTransaction().commitBuilder();
    DataPartMerger merger(&commitBuilder->commitData(), commitBuilder->metadata());
    std::unique_ptr<DataPartBuilder> dataPartBuilder = merger.merge(commitBuilder->commitData().allDataparts());

    DataPart mergedDataPart = DataPart(0, 0);
    mergedDataPart.load(commitBuilder->viewGraph(), *_jobSystem, *dataPartBuilder);

    const auto transaction = writer.openWriteTransaction();
    const auto reader = transaction.readGraph();

    // Confirm that the unmerged node property container has instances of the property
    ASSERT_NE(0, reader.getNodePropertyCount(isRealPropType->_id));
    // Confirm that the merged node property container does not have instances of the property
    ASSERT_EQ(mergedDataPart.nodeProperties().find(isRealPropType->_id), mergedDataPart.nodeProperties().end());
}

TEST_F(DataPartMergerTest, DeleteAllEdgePropertiesAndMerge) {
    auto graph = Graph::create();
    GraphWriter writer {graph.get()};

    createBasicGraph(writer);

    const auto namePropType = graph->openTransaction().viewGraph().metadata().propTypes().get("name");
    const auto proficiencyPropType = graph->openTransaction().viewGraph().metadata().propTypes().get("proficiency");

    writer.deleteEdge(findEdgeIDInWriter<types::String>(writer, "Remy -> Eighties", namePropType->_id));
    writer.deleteEdge(findEdgeIDInWriter<types::String>(writer, "Remy -> Computers", namePropType->_id));
    writer.deleteEdge(findEdgeIDInWriter<types::String>(writer, "Remy -> Ghosts", namePropType->_id));
    writer.deleteEdge(findEdgeIDInWriter<types::String>(writer, "Ghosts -> Remy", namePropType->_id));
    writer.deleteEdge(findEdgeIDInWriter<types::String>(writer, "Maxime -> Paddle", namePropType->_id));
    writer.submit();

    auto* commitBuilder = writer.openWriteTransaction().commitBuilder();
    DataPartMerger merger(&commitBuilder->commitData(), commitBuilder->metadata());
    std::unique_ptr<DataPartBuilder> dataPartBuilder = merger.merge(commitBuilder->commitData().allDataparts());

    DataPart mergedDataPart = DataPart(0, 0);
    mergedDataPart.load(commitBuilder->viewGraph(), *_jobSystem, *dataPartBuilder);

    const auto transaction = writer.openWriteTransaction();
    const auto reader = transaction.readGraph();

    // Confirm that the unmerged edge property container has instances of the property
    ASSERT_NE(0, reader.getEdgePropertyCount(proficiencyPropType->_id));
    // Confirm that the merged edge property container does not have instances of the property
    ASSERT_EQ(mergedDataPart.edgeProperties().find(proficiencyPropType->_id), mergedDataPart.edgeProperties().end());
}
