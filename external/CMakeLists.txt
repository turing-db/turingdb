add_subdirectory(tinyxml2)
add_subdirectory(linenoise)
add_subdirectory(nlohmann_json)
add_subdirectory(cpp-httplib)
add_subdirectory(incbin)
add_subdirectory(crow)

set(neo4j_archive $ENV{TURING}/neo4j/neo4j-3.5.30.tar.gz)
#install(FILES ${neo4j_archive} NAMELINK_ONLY DESTINATION share)
