#pragma once

#include <stddef.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace forge {

// Parsed benchmark workload: an ordered set of named "test loads", each a list
// of one or more queries that are run together as a unit (one timed pass).
// Parsed from a JSON file of the form:
//   { "queries": { "label A": ["q1", "q2"],
//                  "label B": { "queries": ["q1"], "write": true } } }
// A load is either a bare array of queries (a read load) or an object with a
// "queries" array and an optional "write": true flag marking it a write load.
// Test-load order follows the file (insertion order), which matters when one
// load must precede another — e.g. creating data before querying it.
class ForgeWorkload {
public:
    // Bind the workload to a JSON file path; parsing is deferred to parseFile().
    explicit ForgeWorkload(const std::string& path);

    // Parse the bound JSON file into the test loads. Throws TuringException on a missing
    // file, malformed JSON, or a structure that does not match the schema.
    void parseFile();

    size_t getLoadCount() const { return _labels.size(); }
    const std::string& getLabel(size_t index) const { return _labels[index]; }
    const std::vector<std::string>& getQueries(size_t index) const { return _queries[index]; }
    bool isWriteLoad(size_t index) const { return _writeLoads[index]; }

    // Look up a test load by label; returns false (leaving outIndex untouched)
    // if no load has that label.
    bool findLoad(const std::string& label, size_t& outIndex) const;

private:
    std::string _path;
    std::vector<std::string> _labels;
    std::vector<std::vector<std::string>> _queries;
    std::vector<bool> _writeLoads;
    std::unordered_map<std::string, size_t> _labelToIndex;
};

}
