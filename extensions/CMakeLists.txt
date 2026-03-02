
set(extensions_sources
    ExtensionDescriptor.cpp
    ExtensionManager.cpp)

add_library(turing_extensions_s STATIC ${extensions_sources})

target_include_directories(turing_extensions_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_extensions_s PUBLIC
    turing_db_pipeline_s
    ${CMAKE_DL_LIBS})

add_subdirectory(builtin)
