
set(db_sources 
            DB.cpp
            Network.cpp
            DBObject.cpp
            DBIndex.cpp
            Node.cpp
            NodeDescriptor.cpp
            Property.cpp
            Edge.cpp

            ValueType.cpp
            Value.cpp

            NodeType.cpp
            ComponentType.cpp
            EdgeType.cpp

            Writeback.cpp

            DBAccessor.cpp

            DBAccessor.cpp
            DBNetworkRange.cpp
            DBNodeTypeRange.cpp
            DBEdgeTypeRange.cpp
            DBComponentTypeRange.cpp
            NetworkAccessor.cpp
            NetworkNodeRange.cpp
            NetworkEdgeRange.cpp

            StringIndex.cpp
            SharedString.cpp
            StringRef.cpp)

add_library(turing_db_core_s STATIC ${db_sources})
target_include_directories(turing_db_core_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(turing_db_core_s
    common_s)
