#pragma once

#include <cstddef>
#include <algorithm>
#include <immintrin.h>

#include "reader/GraphReader.h"
#include "views/GraphView.h"
#include "views/NodeView.h"
#include "EntityID.h"
#include "PayloadWriter.h"

namespace {

[[maybe_unused]] void fastTolower(const std::string_view& src, std::string& dst) {
    const size_t len = src.size();
    const char* srcStr = src.data();
    char* dstStr = dst.data();

#if defined(__AVX2__) // Check if AVX2 is supported
    if (len < 32) {   // Small strings: Scalar path with unrolling
        size_t i = 0;
        for (; i + 4 <= len; i += 4) {
            dstStr[i] = (srcStr[i] >= 'A' && srcStr[i] <= 'Z') ? srcStr[i] | 0x20 : srcStr[i];
            dstStr[i + 1] = (srcStr[i + 1] >= 'A' && srcStr[i + 1] <= 'Z') ? srcStr[i + 1] | 0x20 : srcStr[i + 1];
            dstStr[i + 2] = (srcStr[i + 2] >= 'A' && srcStr[i + 2] <= 'Z') ? srcStr[i + 2] | 0x20 : srcStr[i + 2];
            dstStr[i + 3] = (srcStr[i + 3] >= 'A' && srcStr[i + 3] <= 'Z') ? srcStr[i + 3] | 0x20 : srcStr[i + 3];
        }
        for (; i < len; ++i) {
            dstStr[i] = (srcStr[i] >= 'A' && srcStr[i] <= 'Z') ? srcStr[i] | 0x20 : srcStr[i];
        }
        return;
    }

    const __m256i lowerA = _mm256_set1_epi8('A');
    const __m256i upperZ = _mm256_set1_epi8('Z');
    const __m256i mask = _mm256_set1_epi8(0x20);

    size_t i = 0;

    // 32 bytes chunks
    for (; i + 31 < len; i += 32) {
        // Load
        const __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&srcStr[i]));

        // >= A && <= Z
        const __m256i cmpA = _mm256_cmpgt_epi8(data, _mm256_sub_epi8(lowerA, _mm256_set1_epi8(1)));
        const __m256i cmpZ = _mm256_cmpgt_epi8(_mm256_add_epi8(data, _mm256_set1_epi8(1)), upperZ);
        const __m256i isUpper = _mm256_and_si256(cmpA, cmpZ);

        // Apply mask
        const __m256i result = _mm256_or_si256(data, _mm256_and_si256(isUpper, mask));

        // Store
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&dstStr[i]), result);
    }

    // Scalar loop for remaining characters
    for (; i < len; ++i) {
        dstStr[i] = (srcStr[i] >= 'A' && srcStr[i] <= 'Z') ? srcStr[i] | 0x20 : srcStr[i];
    }
#else
    // Fallback for systems without AVX2
    size_t i = 0;
    for (; i + 4 <= len; i += 4) {
        dstStr[i] = (srcStr[i] >= 'A' && srcStr[i] <= 'Z') ? srcStr[i] | 0x20 : srcStr[i];
        dstStr[i + 1] = (srcStr[i + 1] >= 'A' && srcStr[i + 1] <= 'Z') ? srcStr[i + 1] | 0x20 : srcStr[i + 1];
        dstStr[i + 2] = (srcStr[i + 2] >= 'A' && srcStr[i + 2] <= 'Z') ? srcStr[i + 2] | 0x20 : srcStr[i + 2];
        dstStr[i + 3] = (srcStr[i + 3] >= 'A' && srcStr[i + 3] <= 'Z') ? srcStr[i + 3] | 0x20 : srcStr[i + 3];
    }
    for (; i < len; ++i) {
        dstStr[i] = (srcStr[i] >= 'A' && srcStr[i] <= 'Z') ? srcStr[i] | 0x20 : srcStr[i];
    }
#endif
}

}

namespace db {

class ListNodesExecutor {
public:
    enum class ExecType : uint8_t {
        Unfiltered = 0,
        PropertyFiltered,
        LabelFiltered,
        PropertyAndLabelFiltered,
    };

    struct PropertyFilter {
        PropertyTypeID _pID;
        std::string _query;
    };

    ListNodesExecutor(const GraphView& view, PayloadWriter& w)
        : _reader(view.read()),
          _propTypes(_reader.getMetadata().propTypes()),
          _writer(w)
    {
    }

    void run() {
        ExecType execType = getExecType();

        switch (execType) {
            case ExecType::Unfiltered:
                processUnfiltered();
                return;
            case ExecType::PropertyFiltered:
                processPropertyFiltered();
                return;
            case ExecType::LabelFiltered:
                processLabelFiltered();
                return;
            case ExecType::PropertyAndLabelFiltered:
                processPropertyAndLabelFiltered();
                return;
        }
    }

    template <ExecType execType>
    void getFilteredIDs(ColumnVector<EntityID>& finalIDs) {
        const auto* firstProp = &_properties.front();
        const size_t needed = _limit + _skip;
        ColumnVector<EntityID> currentChunk; // Used to to filter and generate nextChunk
        ColumnVector<EntityID> nextChunk;    // Used to serve as input for next step
        // TODO: _reachedEnd should be true if we reached the end of the iteration (no more entries if page increased)

        // scanNodeProperties or scanNodePropertiesByLabel
        auto scan = getScanIterator<execType>(firstProp->_pID);

        std::string lower;
        auto query = std::boyer_moore_searcher(firstProp->_query.begin(), firstProp->_query.end());
        while (finalIDs.size() < needed && scan.isValid()) {
            for (; scan.isValid(); scan.next()) {
                const auto& value = scan.get();
                lower.resize(value.size());
                fastTolower(value, lower);

                auto it = std::search(lower.begin(), lower.end(), query);

                if (it != lower.end()) {
                    currentChunk.push_back(scan.getCurrentNodeID());
                    if (currentChunk.size() == needed - finalIDs.size()) {
                        scan.next();
                        break;
                    }
                }
            }

            if (!scan.isValid()) {
                _reachedEnd = true;
            }

            if (_properties.size() == 1) {
                finalIDs = std::move(currentChunk);
                return;
            }

            const auto nextProps = std::span {_properties}.subspan(1);

            for (const auto& [pID, queryStr] : nextProps) {
                auto propIt = _reader.getNodeProperties<types::String>(pID, &currentChunk).begin();

                for (; propIt.isValid(); propIt.next()) {
                    const auto& value = propIt.get();
                    lower.resize(value.size());
                    fastTolower(value, lower);

                    query = std::boyer_moore_searcher(queryStr.begin(), queryStr.end());
                    auto strIt = std::search(lower.begin(), lower.end(), query);

                    if (strIt != lower.end()) {
                        nextChunk.push_back(propIt.getCurrentEntityID());
                    }
                }
                std::swap(currentChunk, nextChunk);
                nextChunk.clear();
            }
            size_t prevSize = finalIDs.size();
            finalIDs.resize(prevSize + currentChunk.size());
            std::copy(currentChunk.begin(), currentChunk.end(), finalIDs.begin() + prevSize);
            currentChunk.clear();
        }
    };

    void processNode(EntityID nodeID) {
        _encounteredNodeCount++;

        if (_encounteredNodeCount <= _skip) {
            return;
        }

        const NodeView node = _reader.getNodeView(nodeID);

        _writer.key(node.nodeID());
        _writer.value(node);
    }

    void addPropertyFilter(PropertyTypeID id, std::string&& str) {
        _properties.emplace_back(id, std::move(str));
    }

    void addLabel(LabelID id) {
        _labelset.set(id);
    }

    void setSkip(size_t skip) { _skip = skip; }
    void setLimit(size_t limit) { _limit = limit; }

    size_t nodeCount() const { return _encounteredNodeCount - _skip; }
    bool reachedEnd() const { return _reachedEnd; }

private:
    static constexpr size_t DEFAULT_LIMIT = 1000;

    size_t _skip {0};
    size_t _limit {DEFAULT_LIMIT};
    GraphReader _reader;
    const PropertyTypeMap& _propTypes;
    size_t _encounteredNodeCount {0};
    PayloadWriter& _writer;
    std::vector<PropertyFilter> _properties;
    LabelSet _labelset;
    bool _reachedEnd {false};

    ExecType getExecType() const {
        if (_properties.empty()) {
            if (_labelset.empty()) {
                return ExecType::Unfiltered;
            }

            return ExecType::LabelFiltered;
        }

        if (_labelset.empty()) {
            return ExecType::PropertyFiltered;
        }

        return ExecType::PropertyAndLabelFiltered;
    }

    void processUnfiltered() {
        size_t i = 0;
        for (const auto nodeID : _reader.scanNodes()) {
            if (i == _limit + _skip) {
                return;
            }
            processNode(nodeID);
            i++;
        }
        _reachedEnd = true;
    }

    void processPropertyFiltered() {
        ColumnVector<EntityID> nodeIDs;

        getFilteredIDs<ExecType::PropertyFiltered>(nodeIDs);

        for (const auto nodeID : nodeIDs) {
            processNode(nodeID);
        }
    }

    void processLabelFiltered() {
        size_t i = 0;
        for (const auto nodeID : _reader.scanNodesByLabel(&_labelset)) {
            if (i == _limit + _skip) {
                return;
            }
            processNode(nodeID);
            i++;
        }
        _reachedEnd = true;
    }

    void processPropertyAndLabelFiltered() {
        ColumnVector<EntityID> nodeIDs;

        getFilteredIDs<ExecType::PropertyAndLabelFiltered>(nodeIDs);

        for (const auto nodeID : nodeIDs) {
            processNode(nodeID);
        }
    }

    template <ExecType type>
    auto getScanIterator(PropertyTypeID pID) const {
        if constexpr (type == ExecType::LabelFiltered || type == ExecType::PropertyAndLabelFiltered) {
            return _reader.scanNodePropertiesByLabel<types::String>(pID, &_labelset).begin();
        } else {
            return _reader.scanNodeProperties<types::String>(pID).begin();
        }
    }
};

}
