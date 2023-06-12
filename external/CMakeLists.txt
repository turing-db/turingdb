add_subdirectory(tinyxml2)
add_subdirectory(linenoise)
add_subdirectory(nlohmann_json)
add_subdirectory(incbin)
add_subdirectory(crow)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/neo4j/neo4j-4.3.23.tar.gz DESTINATION neo4j)
