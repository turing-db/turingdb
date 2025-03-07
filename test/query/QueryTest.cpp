#include <gtest/gtest.h>
#include <tabulate/table.hpp>

#include "LocalMemory.h"
#include "QueryInterpreter.h"
#include "SimpleGraph.h"
#include "TuringDB.h"
#include "columns/Block.h"
#include "spdlog/spdlog.h"

#define COL_2_VEC_CASE(Type, i)                                                \
  case Type::staticKind(): {                                                   \
    const Type &src = *static_cast<const Type *>(col);                         \
    queryResult[i].push_back(src[i]);                                          \
  } break;

using namespace db;

class QueryTest : public ::testing::Test {
  void SetUp() override {
    SimpleGraph::createSimpleGraph(_db);
    _interp = std::make_unique<QueryInterpreter>(&_db.getSystemManager());
  }

  void TearDown() override {}

  TuringDB _db;

public:
  LocalMemory _mem;
  std::unique_ptr<QueryInterpreter> _interp{nullptr};
};

TEST_F(QueryTest, NodeMatching) {
  const std::string query1 = "MATCH n return n";
  const std::string query2 = "MATCH n--m return n,m";
  const std::string query3 = "MATCH n--m--q--r return n,m,q,r";

  _interp->execute(query1, "", &_mem, [](const Block &block) {
    const size_t rowCount = block.getBlockRowCount();

    std::vector<std::vector<EntityID>> queryResult(rowCount);
    const std::vector<std::vector<EntityID>> targetResult = {
        {EntityID(0)},  {EntityID(1)}, {EntityID(2)},  {EntityID(3)},
        {EntityID(4)},  {EntityID(5)}, {EntityID(6)},  {EntityID(7)},
        {EntityID(8)},  {EntityID(9)}, {EntityID(10)}, {EntityID(11)},
        {EntityID(12)},
    };

    for (size_t i = 0; i < rowCount; ++i) {
      for (const Column *col : block.columns()) {
        const ColumnVector<EntityID> &src =
            *static_cast<const ColumnVector<EntityID> *>(col);
        queryResult[i].push_back(src[i]);
      }
    }
    const auto res = queryResult == targetResult;
    if (!res) {
      spdlog::error("We have failed to run query1 in the NodeMatching Test");
    }
    ASSERT_TRUE(res);
  });

  _interp->execute(query2, "", &_mem, [](const Block &block) {
    const size_t rowCount = block.getBlockRowCount();

    std::vector<std::vector<EntityID>> queryResult(rowCount);

    const std::vector<std::vector<EntityID>> targetResult = {
        {EntityID(0), EntityID(1)},  {EntityID(0), EntityID(6)},
        {EntityID(0), EntityID(11)}, {EntityID(0), EntityID(12)},
        {EntityID(1), EntityID(0)},  {EntityID(1), EntityID(7)},
        {EntityID(1), EntityID(8)},  {EntityID(2), EntityID(10)},
        {EntityID(2), EntityID(11)}, {EntityID(3), EntityID(7)},
        {EntityID(3), EntityID(9)},  {EntityID(4), EntityID(8)}};

    for (size_t i = 0; i < rowCount; ++i) {
      for (const Column *col : block.columns()) {
        const ColumnVector<EntityID> &src =
            *static_cast<const ColumnVector<EntityID> *>(col);
        queryResult[i].push_back(src[i]);
      }
    }
    const auto res = queryResult == targetResult;
    if (!res) {
      spdlog::error("We have failed to run query2 in the NodeMatching Test");
    }
    ASSERT_TRUE(res);
  });

  _interp->execute(query3, "", &_mem, [](const Block &block) {
    const size_t rowCount = block.getBlockRowCount();

    std::vector<std::vector<EntityID>> queryResult(rowCount);
    const std::vector<std::vector<EntityID>> targetResult = {
        {EntityID(0), EntityID(1), EntityID(0), EntityID(1)},
        {EntityID(0), EntityID(1), EntityID(0), EntityID(6)},
        {EntityID(0), EntityID(1), EntityID(0), EntityID(11)},
        {EntityID(0), EntityID(1), EntityID(0), EntityID(12)},
        {EntityID(1), EntityID(0), EntityID(1), EntityID(0)},
        {EntityID(1), EntityID(0), EntityID(1), EntityID(7)},
        {EntityID(1), EntityID(0), EntityID(1), EntityID(8)}};

    for (size_t i = 0; i < rowCount; ++i) {
      for (const Column *col : block.columns()) {
        const ColumnVector<EntityID> &src =
            *static_cast<const ColumnVector<EntityID> *>(col);
        queryResult[i].push_back(src[i]);
      }
    }
    const auto res = queryResult == targetResult;
    if (!res) {
      spdlog::error("We have failed to run query3 in the NodeMatching Test");
    }
    ASSERT_TRUE(res);
  });
}

TEST_F(QueryTest, EdgeMatching) {
  const std::string query1 = "MATCH n-[e]-m return e";
  const std::string query2 = "MATCH n-[e]-m-[e1]-q-[e2]-r return e2,e1";

  _interp->execute(query1, "", &_mem, [](const Block &block) {
    const size_t rowCount = block.getBlockRowCount();
    std::vector<std::vector<EntityID>> queryResult(rowCount);
    const std::vector<std::vector<EntityID>> targetResult = {
        {EntityID(0)}, {EntityID(1)}, {EntityID(2)},  {EntityID(3)},
        {EntityID(4)}, {EntityID(5)}, {EntityID(6)},  {EntityID(7)},
        {EntityID(8)}, {EntityID(9)}, {EntityID(10)}, {EntityID(11)}};

    for (size_t i = 0; i < rowCount; ++i) {
      for (const Column *col : block.columns()) {
        const ColumnVector<EntityID> &src =
            *static_cast<const ColumnVector<EntityID> *>(col);
        queryResult[i].push_back(src[i]);
      }
    }
    const auto res = queryResult == targetResult;
    if (!res) {
      spdlog::error("We have failed to run query1 in the EdgeMatching Test");
    }
    ASSERT_TRUE(res);
  });

  _interp->execute(query2, "", &_mem, [](const Block &block) {
    const size_t rowCount = block.getBlockRowCount();
    std::vector<std::vector<EntityID>> queryResult(rowCount);
    const std::vector<std::vector<EntityID>> targetResult = {
        {EntityID(0), EntityID(4)}, {EntityID(1), EntityID(4)},
        {EntityID(2), EntityID(4)}, {EntityID(3), EntityID(4)},
        {EntityID(4), EntityID(0)}, {EntityID(5), EntityID(0)},
        {EntityID(6), EntityID(0)}};

    for (size_t i = 0; i < rowCount; ++i) {
      for (const Column *col : block.columns()) {
        const ColumnVector<EntityID> &src =
            *static_cast<const ColumnVector<EntityID> *>(col);
        queryResult[i].push_back(src[i]);
      }
    }
    const auto res = queryResult == targetResult;
    if (!res) {
      spdlog::error("We have failed to run query2 in the EdgeMatching Test");
    }
    ASSERT_TRUE(res);
  });
}

TEST_F(QueryTest, LabelMatching) {
  const std::string query1 = "MATCH n return n";
  const std::string query2 = "MATCH n:Interests return n";
  const std::string query3 = "MATCH n:Person--m:Person return n";
  const std::string query4 = "MATCH :Person return *";

  _interp->execute(query1, "", &_mem, [](const Block &block) {
    const size_t rowCount = block.getBlockRowCount();
    std::vector<std::vector<EntityID>> queryResult(rowCount);
    const std::vector<std::vector<EntityID>> targetResult = {
        {EntityID(0)}, {EntityID(1)}, {EntityID(2)},
        {EntityID(3)}, {EntityID(4)}, {EntityID(5)}};

    for (size_t i = 0; i < rowCount; ++i) {
      for (const Column *col : block.columns()) {
        const ColumnVector<EntityID> &src =
            *static_cast<const ColumnVector<EntityID> *>(col);
        queryResult[i].push_back(src[i]);
      }
    }
    for (auto rows : queryResult) {
      for (auto cols : rows) {
        spdlog::info("qr {}", cols.getValue());
      }
    }

    for (auto rows : targetResult) {
      for (auto cols : rows) {
        spdlog::info("tr {}", cols.getValue());
      }
    }
    const auto res = queryResult == targetResult;
    if (!res) {
      spdlog::error("We have failed to run query1 in the LabelMatching Test");
    }
    ASSERT_TRUE(res);
  });

  _interp->execute(query2, "", &_mem, [](const Block &block) {
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
    const auto res = queryResult == targetResult;
    if (!res) {
      spdlog::error("We have failed to run query2 in the LabelMatching Test");
    }
    ASSERT_TRUE(res);
  });

  _interp->execute(query3, "", &_mem, [](const Block &block) {
    const size_t rowCount = block.getBlockRowCount();
    std::vector<std::vector<EntityID>> queryResult(rowCount);
    const std::vector<std::vector<EntityID>> targetResult = {
        {EntityID(0)},
        {EntityID(1)},
    };

    for (size_t i = 0; i < rowCount; ++i) {
      for (const Column *col : block.columns()) {
        const ColumnVector<EntityID> &src =
            *static_cast<const ColumnVector<EntityID> *>(col);
        queryResult[i].push_back(src[i]);
      }
    }
    for (auto rows : queryResult) {
      for (auto cols : rows) {
        spdlog::info("qr {}", cols.getValue());
      }
    }

    for (auto rows : targetResult) {
      for (auto cols : rows) {
        spdlog::info("tr {}", cols.getValue());
      }
    }
    const auto res = queryResult == targetResult;
    if (!res) {
      spdlog::error("We have failed to run query3 in the LabelMatching Test");
    }
    ASSERT_TRUE(res);
  });
}
