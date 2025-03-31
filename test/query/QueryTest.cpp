#include <gtest/gtest.h>
#include <optional>
#include <tabulate/table.hpp>

#include "LocalMemory.h"
#include "QueryInterpreter.h"
#include "SimpleGraph.h"
#include "TuringDB.h"
#include "columns/Block.h"

#define COL_2_VEC_CASE(Type, i)                            \
    case Type::staticKind(): {                             \
        const Type &src = *static_cast<const Type *>(col); \
        queryResult[i].push_back(src[i]);                  \
    }                                                      \
    break;

using namespace db;

class QueryTest : public ::testing::Test {
public:
    void SetUp() override {
        SimpleGraph::createSimpleGraph(_db);
        _interp = std::make_unique<QueryInterpreter>(&_db.getSystemManager());
    }

    void TearDown() override {}

protected:
    TuringDB _db;
    LocalMemory _mem;
    std::unique_ptr<QueryInterpreter> _interp{nullptr};
};

TEST_F(QueryTest, NodeMatching) {
    const std::string query1 = "MATCH n return n.name";
    const std::string query2 = "MATCH n--m return n.name,m.name";
    const std::string query3 = "MATCH n--m--q--r return n.name,m.name,q.name,r.name";

    const auto res1 = _interp->execute(query1, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {"Remy"},
            {"Adam"},
            {"Luc"},
            {"Suhas"},
            {"Maxime"},
            {"Martina"},
            {"Ghosts"},
            {"Bio"},
            {"Cooking"},
            {"Paddle"},
            {"Animals"},
            {"Computers"},
            {"Eighties"}};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const ColumnOptVector<types::String::Primitive>& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res1);

    const auto res2 = _interp->execute(query2, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);

        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {"Remy",    "Adam"     },
            {"Remy",    "Ghosts"   },
            {"Remy",    "Computers"},
            {"Remy",    "Eighties" },
            {"Adam",    "Remy"     },
            {"Adam",    "Bio"      },
            {"Adam",    "Cooking"  },
            {"Luc",     "Animals"  },
            {"Luc",     "Computers"},
            {"Maxime",  "Bio"      },
            {"Maxime",  "Paddle"   },
            {"Martina", "Cooking"  },
            {"Ghosts",  "Remy"     }
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const ColumnOptVector<types::String::Primitive>& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res2);

    const auto res3 = _interp->execute(query3, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {"Remy",   "Adam",   "Remy",   "Adam"     },
            {"Remy",   "Adam",   "Remy",   "Ghosts"   },
            {"Remy",   "Adam",   "Remy",   "Computers"},
            {"Remy",   "Adam",   "Remy",   "Eighties" },
            {"Remy",   "Ghosts", "Remy",   "Adam"     },
            {"Remy",   "Ghosts", "Remy",   "Ghosts"   },
            {"Remy",   "Ghosts", "Remy",   "Computers"},
            {"Remy",   "Ghosts", "Remy",   "Eighties" },
            {"Adam",   "Remy",   "Adam",   "Remy"     },
            {"Adam",   "Remy",   "Adam",   "Bio"      },
            {"Adam",   "Remy",   "Adam",   "Cooking"  },
            {"Adam",   "Remy",   "Ghosts", "Remy"     },
            {"Ghosts", "Remy",   "Adam",   "Remy"     },
            {"Ghosts", "Remy",   "Adam",   "Bio"      },
            {"Ghosts", "Remy",   "Adam",   "Cooking"  },
            {"Ghosts", "Remy",   "Ghosts", "Remy"     }
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const ColumnOptVector<types::String::Primitive>& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res3);
}

TEST_F(QueryTest, EdgeMatching) {
    const std::string query1 = "MATCH n-[e]-m return e";
    const std::string query2 = "MATCH n-[e]-m-[e1]-q-[e2]-r return e2, e1";

    const auto res1 = _interp->execute(query1, "", &_mem, [](const Block &block) {
        const size_t rowCount = block.getBlockRowCount();
        std::vector<std::vector<EntityID>> queryResult(rowCount);
        const std::vector<std::vector<EntityID>> targetResult = {
            {EntityID(0)},
            {EntityID(1)},
            {EntityID(2)},
            {EntityID(3)},
            {EntityID(4)},
            {EntityID(5)},
            {EntityID(6)},
            {EntityID(7)},
            {EntityID(8)},
            {EntityID(9)},
            {EntityID(10)},
            {EntityID(11)},
            {EntityID(12)},
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column *col : block.columns()) {
                const ColumnVector<EntityID> &src =
                    *static_cast<const ColumnVector<EntityID> *>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res1);

    const auto res2 = _interp->execute(query2, "", &_mem, [](const Block &block) {
        const size_t rowCount = block.getBlockRowCount();
        std::vector<std::vector<EntityID>> queryResult(rowCount);

        const std::vector<std::vector<EntityID>> targetResult = {
            {EntityID(0),  EntityID(4) },
            {EntityID(1),  EntityID(4) },
            {EntityID(2),  EntityID(4) },
            {EntityID(3),  EntityID(4) },
            {EntityID(0),  EntityID(12)},
            {EntityID(1),  EntityID(12)},
            {EntityID(2),  EntityID(12)},
            {EntityID(3),  EntityID(12)},
            {EntityID(4),  EntityID(0) },
            {EntityID(5),  EntityID(0) },
            {EntityID(6),  EntityID(0) },
            {EntityID(12), EntityID(1) },
            {EntityID(4),  EntityID(0) },
            {EntityID(5),  EntityID(0) },
            {EntityID(6),  EntityID(0) },
            {EntityID(12), EntityID(1) }
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column *col : block.columns()) {
                const ColumnVector<EntityID> &src =
                    *static_cast<const ColumnVector<EntityID> *>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res2);
}
TEST_F(QueryTest, MatchAll) {
    const std::string query1 = "MATCH n return *";
    const std::string query2 = "MATCH n--m return *";

    const auto res1 = _interp->execute(query1, "", &_mem, [](const Block &block) {
        const size_t rowCount = block.getBlockRowCount();
        std::vector<std::vector<EntityID>> queryResult(rowCount);
        const std::vector<std::vector<EntityID>> targetResult = {
            {EntityID(0)}, {EntityID(1)}, {EntityID(2)},  {EntityID(3)},
            {EntityID(4)}, {EntityID(5)}, {EntityID(6)},  {EntityID(7)},
            {EntityID(8)}, {EntityID(9)}, {EntityID(10)}, {EntityID(11)},
            {EntityID(12)}
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column *col : block.columns()) {
                const ColumnVector<EntityID> &src =
                    *static_cast<const ColumnVector<EntityID> *>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res1);

    const auto res2 = _interp->execute(query2, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();
        std::vector<std::vector<EntityID>> queryResult(rowCount);
        const std::vector<std::vector<EntityID>> targetResult = {
            {EntityID(0), EntityID(0),  EntityID(1) },
            {EntityID(0), EntityID(1),  EntityID(6) },
            {EntityID(0), EntityID(2),  EntityID(11)},
            {EntityID(0), EntityID(3),  EntityID(12)},
            {EntityID(1), EntityID(4),  EntityID(0) },
            {EntityID(1), EntityID(5),  EntityID(7) },
            {EntityID(1), EntityID(6),  EntityID(8) },
            {EntityID(2), EntityID(7),  EntityID(10)},
            {EntityID(2), EntityID(8),  EntityID(11)},
            {EntityID(4), EntityID(9),  EntityID(7) },
            {EntityID(4), EntityID(10), EntityID(9) },
            {EntityID(5), EntityID(11), EntityID(8) },
            {EntityID(6), EntityID(12), EntityID(0) }
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column *col : block.columns()) {
                const ColumnVector<EntityID> &src =
                    *static_cast<const ColumnVector<EntityID> *>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res2);
}

TEST_F(QueryTest, LabelMatching) {
    const std::string query1 = "MATCH n:Interests return n.name";
    const std::string query2 = "MATCH n-[e:KNOWS_WELL]-m return n.name";
    const std::string query3 = "MATCH n-[e:KNOWS_WELL]-m return e";

    /*Testing Node LabelSet Matching is pretty hard right now as
      the structure of our label set indexer is an unordered map
      this leads to non determenistic order of the output rows.
      Should look into finishing these tests once labelSetIndexing
      system is modified.*/

    const auto res1 = _interp->execute(query1, "", &_mem, [](const Block &block) {
        const size_t rowCount = block.getBlockRowCount();
        std::vector<std::vector<EntityID>> queryResult(rowCount);
        const std::vector<std::vector<EntityID>> targetResult = {};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column *col : block.columns()) {
                const ColumnVector<EntityID> &src =
                    *static_cast<const ColumnVector<EntityID> *>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res1);

    const auto res2 = _interp->execute(query2, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();
        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {"Remy"}, {"Adam"}, {"Ghosts"}};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const ColumnOptVector<types::String::Primitive>& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res2);

    const auto res3 = _interp->execute(query3, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();
        std::vector<std::vector<EntityID>> queryResult(rowCount);
        const std::vector<std::vector<EntityID>> targetResult = {
            {EntityID(0)}, {EntityID(4)}, {EntityID(12)}};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column *col : block.columns()) {
                const ColumnVector<EntityID> &src =
                    *static_cast<const ColumnVector<EntityID> *>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res3);
}

TEST_F(QueryTest, NodePropertyProjection) {
    const std::string query1 = "MATCH n RETURN n.name";
    const std::string query2 = "MATCH n RETURN n.doesnotexist";

    const auto res1 = _interp->execute(query1, "", &_mem, [](const Block &block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Remy"}}, {{"Adam"}}, {{"Luc"}}, {{"Suhas"}}, {{"Maxime"}}, {{"Martina"}}, {{"Ghosts"}}, {{"Bio"}}, {{"Cooking"}}, {{"Paddle"}}, {{"Animals"}}, {{"Computers"}}, {{"Eighties"}}};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res1);

    const auto res2 = _interp->execute(query2, "", &_mem, [](const Block &block) {});
    ASSERT_FALSE(res2.isOk());
    ASSERT_EQ(res2.getStatus(), QueryStatus::Status::PLAN_ERROR);
}

TEST_F(QueryTest, EdgePropertyProjection) {
    const std::string query1 = "MATCH n-[e]-m RETURN e.name";
    const std::string query2 = "MATCH n-[e]-m RETURN e.doesnotexist";

    const auto res1 = _interp->execute(query1, "", &_mem, [](const Block &block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Remy -> Adam"}}, {{"Remy -> Ghosts"}}, {{"Remy -> Computers"}}, {{"Remy -> Eighties"}}, {{"Adam -> Remy"}}, {{"Adam -> Bio"}}, {{"Adam -> Cooking"}}, {{"Luc -> Animals"}}, {{"Luc -> Computers"}}, {{"Maxime -> Bio"}}, {{"Maxime -> Paddle"}}, {{"Martina -> Cooking"}}, {{"Ghosts -> Remy"}}};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column *col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
    ASSERT_TRUE(res1);

    const auto res2 = _interp->execute(query2, "", &_mem, [](const Block &block) {});
    ASSERT_FALSE(res2.isOk());
    ASSERT_EQ(res2.getStatus(), QueryStatus::Status::PLAN_ERROR);
}

TEST_F(QueryTest, PropertyConstraints) {
    const std::string query1 = "MATCH n:{hasPhD:true} RETURN n.name";
    const std::string query2 = "MATCH n:{hasPhD:true, isFrench: false} RETURN n.name";
    const std::string query3 = "MATCH n:SoftwareEngineering{hasPhD:false} RETURN n.name";
    const std::string query4 = "MATCH n:Person,SoftwareEngineering{hasPhD:false, isFrench: false} RETURN n.name";
    const std::string query5 = "MATCH n-[e:{proficiency:\"expert\"}]-m RETURN n.name,m.name";
    const std::string query6 = "MATCH n-[e:{proficiency:\"expert\",duration:20}]-m RETURN n.name,m.name";
    const std::string query7 = "MATCH n-[e:{proficiency:\"expert\"}]-m:{isReal:true} RETURN n.name,m.name";
    const std::string query8 = "MATCH n-[e:{proficiency:\"expert\"}]-m:SoftwareEngineering{isReal:true} RETURN n.name,m.name";
    const std::string query9 = "MATCH n-[e:KNOWS_WELL{duration:20}]-m:SoftwareEngineering RETURN n.name,m.name";
    const std::string query10 = "MATCH n-[e:INTERESTED_IN{duration:20}]-m:Exotic{isReal:true} RETURN n.name,m.name";

    const auto res1 = _interp->execute(query1, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Remy"}},
            {{"Adam"}},
            {{"Luc"}},
            {{"Martina"}},
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res2 = _interp->execute(query2, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Martina"}}};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res3 = _interp->execute(query3, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Suhas"}}};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res4 = _interp->execute(query4, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Suhas"}}};

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res5 = _interp->execute(query5, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Remy", "Ghosts"}},
            {{"Remy", "Computers"}},
            {{"Maxime", "Paddle"}},
            {{"Ghosts", "Remy"}},
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res6 = _interp->execute(query6, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Remy", "Ghosts"}},
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res7 = _interp->execute(query7, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Remy", "Ghosts"}},
            {{"Remy", "Computers"}},
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res8 = _interp->execute(query8, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Remy", "Computers"}},
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res9 = _interp->execute(query9, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Adam", "Remy"}},
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });

    const auto res10 = _interp->execute(query10, "", &_mem, [](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        std::vector<std::vector<std::optional<types::String::Primitive>>> queryResult(rowCount);
        const std::vector<std::vector<std::optional<types::String::Primitive>>> targetResult = {
            {{"Remy", "Ghosts"}},
        };

        for (size_t i = 0; i < rowCount; ++i) {
            for (const Column* col : block.columns()) {
                const auto& src =
                    *static_cast<const ColumnOptVector<types::String::Primitive>*>(col);
                queryResult[i].push_back(src[i]);
            }
        }

        ASSERT_EQ(queryResult, targetResult);
    });
}
