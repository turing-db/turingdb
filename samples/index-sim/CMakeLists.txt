set(SAMPLE_NAME index-sim)
set(SOURCES index_sim.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

# index_bench builds the three structures for real and times them (~20s at defaults);
# it validates index-sim's cost model rather than being a CI sample, so it is a plain
# executable and stays out of the samples test.list.
add_executable(index-bench index_bench.cpp)

# The ART node-growth path aliases a smaller node over the larger node layout
# (child[numChildren] as the node grows N4->N16->...), which trips a false-positive
# -Warray-bounds under -O3 inlining. Scope the suppression to this benchmark target.
target_compile_options(index-bench PRIVATE -Wno-array-bounds)

install(
    TARGETS     index-bench
    DESTINATION samples/index-sim)
