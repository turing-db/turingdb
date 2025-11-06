#pragma once

#include "Path.h"
#include "TuringTestEnv.h"

namespace db {

using TuringTestEnv = turing::test::TuringTestEnv;

/*
1. Load graph
2. Perform a query
3. Capture output
4. Perform deletion
5. Check output
*/

class DeleteTest {
public:
    DeleteTest(const std::string& testName, const std::string& graph);
    ~DeleteTest() = default;

    DeleteTest(const DeleteTest&) = delete;
    DeleteTest& operator=(const DeleteTest&) = delete;
    DeleteTest(DeleteTest&&) noexcept = delete;
    DeleteTest& operator=(DeleteTest&&) noexcept = delete;

    bool run();

    void addQuery(const std::string& query);

    void deleteNodes(std::vector<NodeID>&& nodes);
    void deleteEdges(std::vector<EdgeID>&& edges);

    static inline const fs::Path WORKING_PATH {SAMPLE_DIR "/.turing"};

private:
    const std::string _testName;
    std::unique_ptr<TuringTestEnv> _env;
    std::string _graphName;
    std::vector<NodeID> _nodesToDelete;
    std::vector<EdgeID> _edgesToDelete;
    std::vector<std::string> _queries;

    void addIncidentEdges(const std::vector<NodeID>& nodes);

    bool runDeleteQueries(bool nodes);
    bool runNodeDeleteQueries() { return runDeleteQueries(true); }
    bool runEdgeDeleteQueries() { return runDeleteQueries(false); }
    void filterBlocks (std::vector<Block>& expectedBlocks);

    template<TypedInternalID IDT>
    void filterColumn(ColumnVector<IDT>* col);
    
};

} // namespace db
