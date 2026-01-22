set(SAMPLE_NAME colops)
set(SOURCES BinaryOperators.h MaskOperators.h main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_storage_s
    turing_db_s
)
