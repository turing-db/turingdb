add_subdirectory(cpphttplib)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/neo4j/neo4j-4.3.23.tar.gz DESTINATION neo4j)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/neo4j/cyber-security-db DESTINATION neo4j)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/neo4j/network-db DESTINATION neo4j)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/neo4j/pole-db DESTINATION neo4j)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/neo4j/stackoverflow-db DESTINATION neo4j)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/neo4j/recommendations-db DESTINATION neo4j)
