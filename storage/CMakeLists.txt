
set(storage_sources
        Graph.cpp
        DataPart.cpp
        DebugDump.cpp
        NodeContainer.cpp
        EdgeContainer.cpp

        metadata/EdgeTypeMap.cpp
        metadata/PropertyTypeMap.cpp
        metadata/LabelMap.cpp
        metadata/LabelSetMap.cpp

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

        indexes/StringIndex.cpp
        indexers/StringPropertyIndexer.cpp

        properties/PropertyManager.cpp

        writers/DataPartBuilder.cpp
        writers/GraphWriter.cpp
        writers/MetadataBuilder.cpp

        versioning/CommitResult.cpp
        versioning/ChangeResult.cpp
        versioning/VersionController.cpp
        versioning/Change.cpp
        versioning/ChangeAccessor.cpp
        versioning/CommitHash.cpp
        versioning/CommitData.cpp
        versioning/Commit.cpp
        versioning/CommitView.cpp
        versioning/CommitBuilder.cpp
        versioning/MetadataRebaser.cpp
        versioning/DataPartRebaser.cpp
        versioning/Transaction.cpp
        versioning/CommitHistory.cpp

        comparators/DataPartComparator.cpp
        comparators/StringIndexerComparator.cpp
        comparators/GraphComparator.cpp)


add_library(turing_db_storage_s STATIC ${storage_sources})

target_include_directories(turing_db_storage_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_storage_s PRIVATE
    turing_db_jobs_s)

target_link_libraries(turing_db_storage_s PUBLIC
        turing_db_base_s
        turing_common_s
        range-v3)
