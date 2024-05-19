
set(biollm_sources 
        BioLLM.cpp)

add_executable(biollm ${biollm_sources})
target_link_libraries(biollm PRIVATE
    turing_mind_modelbuilder_s
    tools_common_s)

install(TARGETS biollm DESTINATION bin)
