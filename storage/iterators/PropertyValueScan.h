#pragma once

#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <type_traits>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#include "ID.h"

namespace db {

template <typename Lane>
struct PropertyValueScanLanes;

template <>
struct PropertyValueScanLanes<int64_t> {
    using Vector = int64_t __attribute__((vector_size(32)));
};

template <>
struct PropertyValueScanLanes<uint64_t> {
    using Vector = uint64_t __attribute__((vector_size(32)));
};

template <>
struct PropertyValueScanLanes<double> {
    using Vector = double __attribute__((vector_size(32)));
};

template <typename Lane>
concept VectorisedPropertyLane = requires { typename PropertyValueScanLanes<Lane>::Vector; };

// Equality kernels behind the property value scan: given a run of a property column and
// the entity IDs beside it, write the IDs whose value equals the needle.
class PropertyValueScan {
public:
    template <typename Primitive>
    static size_t equalScalar(const Primitive* values, const EntityID* ids, size_t rows, const Primitive& needle, NodeID* hits) {
        return equalScalarFrom(values, StoredIDs {ids}, rows, needle, hits);
    }

    template <VectorisedPropertyLane Lane>
    static size_t equalVectorised(const Lane* values, const EntityID* ids, size_t rows, const Lane& needle, NodeID* hits) {
        return equalVectorisedFrom(values, StoredIDs {ids}, rows, needle, hits);
    }

    template <typename Primitive>
    static size_t equal(const Primitive* values, const EntityID* ids, size_t rows, const Primitive& needle, NodeID* hits) {
        return equalFrom(values, StoredIDs {ids}, rows, needle, hits);
    }

    // The run of a sorted gap-free container holds the ID of row r at baseID + r, so the
    // second eight bytes per row are computed instead of read and the kernel streams the
    // values alone.
    template <typename Primitive>
    static size_t equalDenseIDs(const Primitive* values, uint64_t baseID, size_t rows, const Primitive& needle, NodeID* hits) {
        return equalFrom(values, DenseIDs {baseID}, rows, needle, hits);
    }

private:
    using EqualityMask = int64_t __attribute__((vector_size(32)));

    static constexpr size_t _laneCount = 4;

    // Where a matched row's ID comes from: the array stored beside the values, or the
    // arithmetic that replaces it over a dense run.
    struct StoredIDs {
        const EntityID* _ids {nullptr};

        uint64_t at(size_t row) const { return _ids[row].getValue(); }
        StoredIDs advanced(size_t rows) const { return StoredIDs {_ids + rows}; }

#if defined(__AVX2__)
        __m256i vector(size_t row) const { return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(_ids + row)); }
#endif
    };

    struct DenseIDs {
        uint64_t _baseID {0};

        uint64_t at(size_t row) const { return _baseID + row; }
        DenseIDs advanced(size_t rows) const { return DenseIDs {_baseID + rows}; }

#if defined(__AVX2__)
        __m256i vector(size_t row) const {
            const __m256i ramp = _mm256_setr_epi64x(0, 1, 2, 3);
            return _mm256_add_epi64(_mm256_set1_epi64x(static_cast<int64_t>(_baseID + row)), ramp);
        }
#endif
    };

    // Every row's ID is stored and the cursor advances by the comparison, so a run of
    // misses costs no mispredicted branch.
    template <typename Primitive, typename IDSource>
    static size_t equalScalarFrom(const Primitive* values, IDSource ids, size_t rows, const Primitive& needle, NodeID* hits) {
        size_t count = 0;

        for (size_t row = 0; row < rows; row++) {
            hits[count] = NodeID {ids.at(row)};
            count += static_cast<size_t>(values[row] == needle);
        }

        return count;
    }

    template <VectorisedPropertyLane Lane, typename IDSource>
    static size_t equalVectorisedFrom(const Lane* values, IDSource ids, size_t rows, const Lane& needle, NodeID* hits) {
        using Vector = PropertyValueScanLanes<Lane>::Vector;
        static_assert(sizeof(Vector) / sizeof(Lane) == _laneCount);

        constexpr size_t blockRows = _vectorsPerBlock * _laneCount;

        Vector needles {};
        for (size_t lane = 0; lane < _laneCount; lane++) {
            needles[lane] = needle;
        }

        size_t count = 0;
        size_t row = 0;

        for (; row + blockRows <= rows; row += blockRows) {
            EqualityMask matches[_vectorsPerBlock];
            EqualityMask any {};

            for (size_t block = 0; block < _vectorsPerBlock; block++) {
                Vector chunk;
                memcpy(&chunk, values + row + block * _laneCount, sizeof(Vector));

                matches[block] = (chunk == needles);
                any |= matches[block];
            }

            if (!anyLaneMatches(any)) {
                continue;
            }

            for (size_t block = 0; block < _vectorsPerBlock; block++) {
                count += compactMatchedIDs(ids, row + block * _laneCount, matches[block], hits + count);
            }
        }

        return count + equalScalarFrom(values + row, ids.advanced(row), rows - row, needle, hits + count);
    }

    template <typename Primitive, typename IDSource>
    static size_t equalFrom(const Primitive* values, IDSource ids, size_t rows, const Primitive& needle, NodeID* hits) {
        if constexpr (VectorisedPropertyLane<Primitive>) {
            return equalVectorisedFrom(values, ids, rows, needle, hits);
        } else {
            return equalScalarFrom(values, ids, rows, needle, hits);
        }
    }

#if defined(__AVX2__)
    static constexpr size_t _vectorsPerBlock = 8;

    // Per comparison mask, the 32-bit permute lanes that pack the matching 64-bit IDs to
    // the front of the vector.
    static constexpr uint32_t _compactionLanes[1 << _laneCount][2 * _laneCount] {
        {0, 1, 0, 1, 0, 1, 0, 1},
        {0, 1, 0, 1, 0, 1, 0, 1},
        {2, 3, 0, 1, 0, 1, 0, 1},
        {0, 1, 2, 3, 0, 1, 0, 1},
        {4, 5, 0, 1, 0, 1, 0, 1},
        {0, 1, 4, 5, 0, 1, 0, 1},
        {2, 3, 4, 5, 0, 1, 0, 1},
        {0, 1, 2, 3, 4, 5, 0, 1},
        {6, 7, 0, 1, 0, 1, 0, 1},
        {0, 1, 6, 7, 0, 1, 0, 1},
        {2, 3, 6, 7, 0, 1, 0, 1},
        {0, 1, 2, 3, 6, 7, 0, 1},
        {4, 5, 6, 7, 0, 1, 0, 1},
        {0, 1, 4, 5, 6, 7, 0, 1},
        {2, 3, 4, 5, 6, 7, 0, 1},
        {0, 1, 2, 3, 4, 5, 6, 7},
    };

    static_assert(std::is_trivially_copyable_v<EntityID>);
    static_assert(std::is_trivially_copyable_v<NodeID>);
    static_assert(sizeof(EntityID) == sizeof(uint64_t));
    static_assert(sizeof(NodeID) == sizeof(uint64_t));

    static bool anyLaneMatches(EqualityMask matches) {
        const __m256i lanes = (__m256i)matches;
        return _mm256_testz_si256(lanes, lanes) == 0;
    }

    // The whole vector of IDs is stored and the cursor advances by the population count,
    // so the misses trailing the last match are overwritten by the next group. The cursor
    // never runs ahead of the rows already read, which keeps that store inside the
    // caller's buffer.
    template <typename IDSource>
    static size_t compactMatchedIDs(const IDSource& ids, size_t row, EqualityMask matches, NodeID* hits) {
        const int mask = _mm256_movemask_pd(_mm256_castsi256_pd((__m256i)matches));
        if (mask == 0) {
            return 0;
        }

        const __m256i loaded = ids.vector(row);
        const __m256i lanes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(_compactionLanes[mask]));

        _mm256_storeu_si256(reinterpret_cast<__m256i*>(hits), _mm256_permutevar8x32_epi32(loaded, lanes));

        return static_cast<size_t>(__builtin_popcount(static_cast<unsigned>(mask)));
    }
#else
    static constexpr size_t _vectorsPerBlock = 16;

    static bool anyLaneMatches(EqualityMask matches) {
        return (matches[0] | matches[1] | matches[2] | matches[3]) != 0;
    }

    template <typename IDSource>
    static size_t compactMatchedIDs(const IDSource& ids, size_t row, EqualityMask matches, NodeID* hits) {
        size_t count = 0;

        for (size_t lane = 0; lane < _laneCount; lane++) {
            hits[count] = NodeID {ids.at(row + lane)};
            count += static_cast<size_t>(matches[lane] != 0);
        }

        return count;
    }
#endif
};

}
