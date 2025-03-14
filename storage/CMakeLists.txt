
set(storage_sources
        Graph.cpp
        DataPart.cpp
        DataPartManager.cpp
        DebugDump.cpp
        NodeContainer.cpp
        EdgeContainer.cpp

        types/EdgeTypeMap.cpp
        types/PropertyTypeMap.cpp

        labels/LabelMap.cpp

        views/GraphView.cpp
        views/NodeEdgeView.cpp

        reader/GraphReader.cpp

        iterators/GetInEdgesIterator.cpp
        iterators/GetPropertiesIterator.cpp
        iterators/GetOutEdgesIterator.cpp
        iterators/GetNodeViewsIterator.cpp
        iterators/PartIterator.cpp
        iterators/ScanEdgesIterator.cpp
        iterators/ScanEdgePropertiesIterator.cpp
        iterators/ScanInEdgesByLabelIterator.cpp
        iterators/ScanNodePropertiesIterator.cpp
        iterators/ScanNodePropertiesByLabelIterator.cpp
        iterators/ScanNodesByLabelIterator.cpp
        iterators/ScanNodesIterator.cpp
        iterators/ScanOutEdgesByLabelIterator.cpp
        iterators/MatchLabelSetIterator.cpp

        columns/Block.cpp
        columns/Column.cpp

        indexers/EdgeIndexer.cpp

        properties/PropertyManager.cpp

        writers/DataPartBuilder.cpp
        writers/ConcurrentWriter.cpp

        comparators/DataPartComparator.cpp
        comparators/GraphComparator.cpp)


add_library(turing_db_storage_s STATIC ${storage_sources})
target_include_directories(turing_db_storage_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_storage_s PRIVATE
    turing_db_jobs_s)

target_link_libraries(turing_db_storage_s PUBLIC
        turing_db_base_s
        turing_common_s
        range-v3)
