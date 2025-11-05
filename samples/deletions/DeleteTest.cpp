#include "DeleteTest.h"

#include <bits/ranges_algo.h>
#include <spdlog/spdlog.h>
#include <vector>

#include "ID.h"
#include "SystemManager.h"
#include "columns/Block.h"
#include "columns/ColumnVector.h"
#include "versioning/Change.h"

DeleteTest::DeleteTest(const std::string& graph)
    : _graphName {graph}
{
}

void DeleteTest::addQuery(const std::string& query) {
    _queries.emplace_back(query);
}

void DeleteTest::deleteNodes(std::vector<NodeID>&& nodes) {
    _nodesToDelete.insert(_nodesToDelete.cend(),nodes.begin(), nodes.end());
}

void DeleteTest::deleteEdges(std::vector<EdgeID>&& edges) {
    _edgesToDelete.insert(_edgesToDelete.cend(),edges.begin(), edges.end());
}

bool DeleteTest::runDeleteQueries(bool nodes) {
    TuringDB& db = _env->getDB();
    const std::string type = nodes ? "nodes" : "edges";

    std::string deleteQuery = "delete " + type + " ";

    if (nodes) {
        for (NodeID node : _nodesToDelete) {
            deleteQuery += std::to_string(node.getValue()) + ", ";
        }
    } else {
        for (EdgeID edge : _edgesToDelete) {
            deleteQuery += std::to_string(edge.getValue()) + ", ";
        }
    }
    deleteQuery.pop_back(); // Remove extra space
    deleteQuery.pop_back(); // Remove extra comma

    auto res = db.getSystemManager().newChange(_graphName);
    if (!res) {
        spdlog::error("Failed to make change to delete {}", type);
        return false;
    }
    Change* ch = res.value();
    if (auto qres = db.query(deleteQuery, _graphName, &_env->getMem(),
                             CommitHash::head(), ch->id());
        !res) {
        spdlog::error("Failed to delete {}: {}", type, qres.getError());
        return false;
    }
    if (!db.query("change submit", _graphName, &_env->getMem(), CommitHash::head(),
                  ch->id())) {
        spdlog::error("Failed to submit {} deletion change.", type);
        return false;
    }
    spdlog::info("Ran {}", deleteQuery);
    return true;
}

template <TypedInternalID IDT>
void DeleteTest::filterColumn(ColumnVector<IDT>* col) {
    std::vector<IDT>& raw = col->getRaw();
    if constexpr (std::is_same_v<IDT, NodeID>) {
        std::erase_if(raw, [this](NodeID id) {
            return std::ranges::binary_search(_nodesToDelete, id);
        });
    } else {
        std::erase_if(raw, [this](EdgeID id) {
            return std::ranges::binary_search(_edgesToDelete, id);
        });
    }
}

void DeleteTest::filterBlocks(std::vector<Block>& expectedBlocks) {
    for (Block& b : expectedBlocks) {
        for (Column* c : b.columns()) {
            if (c->getKind() == ColumnVector<NodeID>::staticKind()) {
                filterColumn(c->cast<ColumnVector<NodeID>>());
            }
            else {
                filterColumn(c->cast<ColumnVector<EdgeID>>());
            }
        }
    }
}

bool DeleteTest::run() {
    const fs::Path path {SAMPLE_DIR "/.turing"};
    _env = TuringTestEnv::create(path);

    SystemManager& sysMan = _env->getSystemManager();
    TuringDB& db = _env->getDB();

    if (!sysMan.loadGraph(_graphName)) {
        spdlog::error("Failed to load graph {}", _graphName);
        return false;
    }

    // Perform queries, get outputs as blocks
    std::vector<Block> queryOutputBlocks;
    Block thisBlock;
    for (const auto& query : _queries) {
        thisBlock.clear();
        const auto COPY_BLOCK = [&thisBlock](const Block& block){
            thisBlock.append(block);
        };
        db.query(query, _graphName, &_env->getMem(), COPY_BLOCK, CommitHash::head(), ChangeID::head());
        queryOutputBlocks.push_back(std::move(thisBlock));
    }

    for (Block& block : queryOutputBlocks) {
        if (block.empty()) {
            continue;
        }
        for (Column* col : block.columns()) {
            if (col->getKind() != ColumnVector<NodeID>::staticKind()
                && col->getKind() != ColumnVector<EdgeID>::staticKind()) {
                spdlog::error(
                    "DeleteTest only supports queries which output Node/EdgeID columns.");
                return false;
            }
        }
    }

    if (!_nodesToDelete.empty()) {
        runNodeDeleteQueries();
    }
    if (!_edgesToDelete.empty()) {
        runEdgeDeleteQueries();
    }

    std::ranges::sort(_nodesToDelete);
    std::ranges::sort(_edgesToDelete);

    std::vector<Block> expectedBlocks;
    expectedBlocks.reserve(queryOutputBlocks.size());
    for (auto& block : queryOutputBlocks) {
        expectedBlocks.emplace_back(std::move(block));
    }
    filterBlocks(expectedBlocks);

    for (size_t i {0}; const auto& query : _queries) {
        auto& thisExpectedBlock = expectedBlocks[i];
        bool same = false;
        const auto ASSERT_EQ = [&](const Block& block) -> void {
            const auto PRINT_COL = [](ColumnVector<EntityID>* col) {
                for (auto id : *col) {
                    spdlog::info("\t{}", id);
                }
            };

            if (block.size() != thisExpectedBlock.size()) {
                spdlog::error("Block sizes: Expected: {}; Actual: {}",
                              thisExpectedBlock.size(), block.size());
                same = false;
                return;
            }
            if (block.empty() && thisExpectedBlock.empty()) {
                same = true;
                return;
            }

            for (size_t i = 0; i < block.size(); i++) {
                ColumnVector<EntityID>* blockCol =
                    block.columns()[i]->cast<ColumnVector<EntityID>>();
                ColumnVector<EntityID>* expCol =
                    thisExpectedBlock.columns()[i]->cast<ColumnVector<EntityID>>();

                if (blockCol->size() != expCol->size()) {
                    spdlog::error("Column {} sizes: Expected: {}; Actual: {}", i,
                                  expCol->size(), blockCol->size());
                    spdlog::info("Expected:");
                    PRINT_COL(expCol);
                    spdlog::info("Actual:");
                    PRINT_COL(blockCol);
                    same = false;
                    return;
                }

                for (size_t j = 0; j < blockCol->size(); j++) {
                    if (blockCol->at(j) != expCol->at(j)) {
                        spdlog::error("Column {} index {}: Expected: {}; Actual: {}", i,
                                      j, expCol->at(j), blockCol->at(j));
                        same = false;
                        return;
                    }
                }
            }
            same = true;
        };

        if (auto res = db.query(query, _graphName, &_env->getMem(), ASSERT_EQ,
                                CommitHash::head(), ChangeID::head());
            !res) {
            spdlog::error("Failed to run query {}: {}", query, res.getError());
            return false;
        }
        if (!same) {
            spdlog::error("Blocks at index {} are not the same", i);
            return false;
        }
    }

    return true;
}
