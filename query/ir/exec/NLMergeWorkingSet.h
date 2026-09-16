#pragma once

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "versioning/CommitWriteBuffer.h"

#include "NLProgram.h"

namespace db {

// A merge chain matched as far as one node, and the entities it bound getting there
struct NLMergePartialMatch {
    std::vector<NLMergeRef> _nodes;
    std::vector<NLMergeRef> _edges;
};

// One hop a partial match's far node can be extended along: the edge and the node it
// lands on
struct NLMergeExtension {
    NLMergeRef _source;
    NLMergeRef _edge;
    NLMergeRef _target;
};

// The scratch one step of an nl.merge works in: the candidates of each chain node, the
// frontier of the chain walk, the edges a hop can extend it along and the keys it looks
// entities up under. Owned by the op's data, so a merge driven by many chunks allocates
// it once rather than once per chunk.
struct NLMergeWorkingSet {
    // The values one entity spec asks for, extracted once for the whole chunk so a
    // written row picks its own by row index
    using PropertiesPerRow = std::vector<CommitWriteBuffer::UntypedProperties>;

    NLMergeWorkingSet();
    ~NLMergeWorkingSet();

    // Per chain node, the current row's candidates, and their refs as a set for the
    // membership test each hop's far end has to pass
    std::vector<std::vector<NLMergeRef>> _candidates;
    std::vector<std::unordered_set<uint64_t>> _candidateKeys;

    std::vector<PropertiesPerRow> _nodeProperties;
    std::vector<PropertiesPerRow> _hopProperties;

    // The frontier of the chain walk and the one the current hop extends it into
    std::vector<NLMergePartialMatch> _matches;
    std::vector<NLMergePartialMatch> _extended;

    // The current hop's candidate graph edges, the ones of them carrying the property
    // values the hop asks for, and where each source node's run of them starts and ends
    // once they are grouped
    std::vector<NLMergeExtension> _extensions;
    std::vector<NLMergeExtension> _keptExtensions;
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> _extensionRuns;

    // The nodes a hop scans out of, deduplicated: two partial matches reaching the same
    // node scan its edges once
    std::unordered_set<uint64_t> _scanSourceKeys;

    // A chain node's lookup key, a hop's, that same key behind the hop's signature -
    // which is what the pending log is keyed by - and the key a candidate edge's own
    // values serialize into
    std::string _key;
    std::string _hopKey;
    std::string _pendingHopKey;
    std::string _scanKey;
};

}
