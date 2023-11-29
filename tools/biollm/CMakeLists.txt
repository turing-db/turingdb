
set(biollm_sources 
        BioLLM.cpp)

add_executable(biollm ${biollm_sources})
target_link_libraries(biollm PRIVATE
    turing_mind_modelbuilder_s
    tools_common_s)

target_include_directories(biollm PRIVATE ${BIO_SOURCE_DIR}/messages)

install(TARGETS biollm DESTINATION bin)
