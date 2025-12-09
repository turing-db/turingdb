set(SAMPLE_NAME vector-db)
set(SOURCES
    main.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
  PRIVATE  turing_vector_s)
