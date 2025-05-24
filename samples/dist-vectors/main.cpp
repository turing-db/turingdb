#include <vector>
#include <math.h>
#include <time.h>
#include <iostream>

#include "Time.h"

struct EVector {
    static constexpr size_t DIM = 128;
    float _data[DIM];
};

float getDistance(const EVector& lhs, const EVector& rhs) {
    const auto& dataLHS = lhs._data;
    const auto& dataRHS = rhs._data;

    float dist = 0.0f;
    for (size_t i = 0; i < EVector::DIM; i++) {
        const float diff = (dataRHS[i]-dataLHS[i]);
        const float square = diff*diff;
        dist += square;
    }

    return sqrtf(dist);
}

size_t computeMin(const std::vector<EVector>& embeddings,
                  const EVector& query) {
    size_t minIndex = 0;
    float minDistance = std::numeric_limits<float>::max();
    const size_t embeddingsCount = embeddings.size();
    for (size_t i = 1; i < embeddingsCount; i++) {
        const EVector& current = embeddings[i];
        const float dist = getDistance(current, query);
        if (dist < minDistance) {
            minDistance = dist;
            minIndex = i;
        }
    }

    return minIndex;
}

float randomFloat() {
    return (float)(rand())/(float)(rand());
}

void generateVector(EVector& vec) {
    for (float& value : vec._data) {
        value = randomFloat();
    }
}

void test() {
    srand(time(NULL));

    // Generate vectors
    constexpr size_t vectorsCount = 1000000;
    std::vector<EVector> embeddings;
    embeddings.resize(vectorsCount);
    for (size_t i = 0; i < vectorsCount; i++) {
        generateVector(embeddings[i]);
    }

    // Generate query
    EVector query;
    generateVector(query);

    // Compute min
    const TimePoint start = Clock::now();
    const auto min = computeMin(embeddings, query);
    const TimePoint end = Clock::now();

    std::cout << "min=" << min << '\n';
    std::cout << duration<Milliseconds>(start, end) << '\n';
}

int main() {
    test();
    return 0;
}
