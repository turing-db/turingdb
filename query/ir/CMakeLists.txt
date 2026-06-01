set(SOURCES
    Test.cpp
)

add_library(turing_ir_s STATIC ${SOURCES})

target_link_libraries(turing_ir_s
    PUBLIC
    mlir
)
