set(storage_sources
        Graph.cpp
        ChangeManager.cpp
        DebugDump.cpp
        RowStore.cpp
        EntityList.cpp

        buffers/BufferChunk.cpp
        buffers/RawBuffer.cpp
        buffers/SpanBuffer.cpp
        buffers/StringBuffer.cpp

        datapart/DataPart.cpp
        datapart/NodeContainer.cpp
        datapart/EdgeContainer.cpp

        embedding/EmbeddingBucket.cpp
        embedding/EmbeddingContainer.cpp

        list/ListByteBuffer.cpp
        list/ListBuffer.cpp
        list/ListElementOrder.cpp
        list/ListElementViewBuffer.cpp
        list/ListWriteCursor.cpp

        metadata/EdgeTypeMap.cpp
        metadata/PropertyTypeMap.cpp
        metadata/LabelMap.cpp
        metadata/LabelSetMap.cpp

        views/GraphView.cpp
        views/NodeEdgeView.cpp

        reader/GraphReader.cpp

        iterators/GetInEdgesIterator.cpp
        iterators/GetInEdgesByTypeIterator.cpp
        iterators/GetPropertiesIterator.cpp
        iterators/GetPropertiesWithNullIterator.cpp
        iterators/GetNodeLabelSetIterator.cpp
        iterators/GetOutEdgesIterator.cpp
        iterators/GetOutEdgesByTypeIterator.cpp
        iterators/GetEdgesIterator.cpp
        iterators/ScanEdgesByTypeIterator.cpp
        iterators/GetNodeViewsIterator.cpp
        iterators/PartIterator.cpp
        iterators/ScanEdgesIterator.cpp
        iterators/ScanEdgePropertiesIterator.cpp
        iterators/ScanInEdgesByLabelIterator.cpp
        iterators/ScanNodePropertiesIterator.cpp
        iterators/ScanNodePropertiesByLabelIterator.cpp
        iterators/ScanNodesByLabelIterator.cpp
        iterators/ScanNodesByPropertyValueIterator.cpp
        iterators/ScanNodesIterator.cpp
        iterators/ScanLabelsIterator.cpp
        iterators/ScanEdgeTypesIterator.cpp
        iterators/ScanPropertyTypesIterator.cpp
        iterators/ScanOutEdgesByLabelIterator.cpp
        iterators/MatchLabelSetIterator.cpp
        iterators/NeighbourhoodSampleIterator.cpp

        iterators/TombstoneFilter.cpp

        columns/Block.cpp
        columns/Column.cpp
        columns/ColumnConst.cpp
        columns/ColumnStringTable.cpp
        columns/Functions.cpp

        dataframe/Dataframe.cpp
        dataframe/NamedColumn.cpp
        dataframe/DataframeManager.cpp

        indexers/EdgeIndexer.cpp

        indexes/StringIndex.cpp
        indexers/StringPropertyIndexer.cpp

        indexes/IndexManager.cpp

        properties/PropertyContainer.cpp
        properties/PropertyManager.cpp

        indexes/PropertyHashIndex.cpp

        writers/DataPartBuilder.cpp
        writers/GraphWriter.cpp
        writers/MetadataBuilder.cpp

        mergers/DataPartMerger.cpp
        mergers/DataPartMergeResult.cpp

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
        versioning/ChangeRebaser.cpp
        versioning/ChangeConflictChecker.cpp
        versioning/MetadataRebaser.cpp
        versioning/DataPartRebaser.cpp
        versioning/EntityIDRebaser.cpp
        versioning/Transaction.cpp
        versioning/CommitHistory.cpp
        versioning/CommitHistoryBuilder.cpp
        versioning/CommitHistoryRebaser.cpp
        versioning/CommitWriteBuffer.cpp
        versioning/CommitJournal.cpp
        versioning/WriteSet.cpp
        versioning/Tombstones.cpp
        versioning/TombstoneSet.cpp
        versioning/VersionControlException.cpp
        versioning/TombstoneRanges.cpp

        comparators/DataPartComparator.cpp
        comparators/StringIndexerComparator.cpp
        comparators/CommitViewComparator.cpp
        comparators/WriteSetComparator.cpp
        comparators/TombstoneSetComparator.cpp
        comparators/GraphComparator.cpp

        dump/GraphLoader.cpp
        dump/GraphDumper.cpp
        dump/DataPartLoader.cpp
        dump/CommitDumper.cpp
        dump/CommitLoader.cpp
        dump/DataPartDumper.cpp
        dump/DumpResult.cpp
        dump/LoadGraphResult.cpp
        dump/DumpUtils.cpp
        dump/LoadUtils.cpp
        dump/StringIndexerDumper.cpp
        dump/StringIndexerLoader.cpp
        dump/EmbeddingPropertyContainerDumper.cpp
        dump/EmbeddingPropertyContainerLoader.cpp
        dump/CommitJournalDumper.cpp
        dump/CommitJournalLoader.cpp
        dump/CommitMetaDataDumper.cpp
        dump/TombstonesDumper.cpp
        dump/TombstonesLoader.cpp
      )

add_library(turing_db_storage_s STATIC ${storage_sources})

target_include_directories(turing_db_storage_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}
            ${BUILD_INFO_INCLUDE_DIR})

target_link_libraries(turing_db_storage_s
    PRIVATE turing_db_jobs_s
    PUBLIC  turing_db_base_s
            turing_db_io_fs_s
            turing_common_s
            range-v3)
