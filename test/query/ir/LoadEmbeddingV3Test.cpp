#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using CountColumn = ColumnVector<types::UInt64::Primitive>;

class CountSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {
        _columnNames.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const CountColumn* const counts = static_cast<const CountColumn*>(chunks.front());

        for (size_t row = offset; row < offset + rowCount; row++) {
            _counts.push_back((*counts)[row]);
        }
    }

    const std::vector<std::string>& getColumnNames() const { return _columnNames; }
    const std::vector<uint64_t>& getCounts() const { return _counts; }

private:
    std::vector<std::string> _columnNames;
    std::vector<uint64_t> _counts;
};

// Reads back the embedding property a load wrote: a node ID chunk beside a nullable
// vector chunk, one row per node the query matched.
class EmbeddingSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, std::optional<std::vector<float>>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        using EmbeddingColumn = ColumnOptVector<std::span<const float>>;

        const ColumnNodeIDs* const nodeIDs = static_cast<const ColumnNodeIDs*>(chunks[0]);
        const EmbeddingColumn* const values = static_cast<const EmbeddingColumn*>(chunks[1]);

        const std::vector<NodeID>& rawIDs = nodeIDs->getRaw();
        const std::vector<std::optional<std::span<const float>>>& rawValues = values->getRaw();

        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::vector<float>> value;
            if (rawValues[row]) {
                value.emplace(rawValues[row]->begin(), rawValues[row]->end());
            }

            _rows.emplace_back(rawIDs[row].getValue(), value);
        }
    }

    // A node missing from the result and a node holding a null value are different
    // answers, so the two are reported apart rather than both as an empty optional.
    bool findValue(uint64_t nodeID, std::optional<std::vector<float>>& value) const {
        const auto row = std::ranges::find_if(_rows, [nodeID](const Row& candidate) {
            return candidate.first == nodeID;
        });

        if (row == _rows.end()) {
            return false;
        }

        value = row->second;
        return true;
    }

    size_t countLoaded() const {
        return std::ranges::count_if(_rows, [](const Row& row) { return row.second.has_value(); });
    }

private:
    std::vector<Row> _rows;
};

void expectVector(const EmbeddingSink& sink, uint64_t nodeID, const std::vector<float>& expected) {
    std::optional<std::vector<float>> value;
    ASSERT_TRUE(sink.findValue(nodeID, value)) << "node " << nodeID << " is not in the result";
    ASSERT_TRUE(value) << "node " << nodeID << " carries no embedding";

    ASSERT_EQ(value->size(), expected.size()) << "node " << nodeID;

    for (size_t element = 0; element < expected.size(); element++) {
        EXPECT_FLOAT_EQ((*value)[element], expected[element])
            << "node " << nodeID << " element " << element;
    }
}

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {}
};

const std::vector<std::vector<float>> loadedVectors {
    {1.0f, 0.25f, 0.5f, 0.75f},
    {2.0f, 0.5f, 1.0f, 1.5f},
    {3.0f, 0.75f, 1.5f, 2.25f},
};

}

// LOAD EMBEDDING runs through the MLIR engine as a db.load_embedding op, staging one
// property write per row of the Parquet file into the change's write buffer. It writes,
// so every case here runs inside a change of its own. The node IDs the files name are
// simpledb's first three - Remy, Adam and Computers.
class LoadEmbeddingV3Test : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    // Writes the layout LOAD EMBEDDING reads - an INT64 node_id column beside a
    // FIXED_LEN_BYTE_ARRAY embedding column of little-endian float32 - into the data
    // directory, and returns the bare name the query names it by.
    std::string writeEmbeddingParquet(const std::string& name,
                                      const std::vector<int64_t>& nodeIDs,
                                      const std::vector<std::vector<float>>& vectors) {
        const size_t byteWidth = vectors.front().size() * sizeof(float);

        std::vector<uint8_t> bytes;
        bytes.reserve(vectors.size() * byteWidth);
        for (const std::vector<float>& vector : vectors) {
            const uint8_t* const raw = reinterpret_cast<const uint8_t*>(vector.data());
            bytes.insert(bytes.end(), raw, raw + vector.size() * sizeof(float));
        }

        parquet::schema::NodeVector fields;
        fields.push_back(parquet::schema::PrimitiveNode::Make("node_id",
                                                              parquet::Repetition::REQUIRED,
                                                              parquet::Type::INT64));
        fields.push_back(parquet::schema::PrimitiveNode::Make("embedding",
                                                              parquet::Repetition::REQUIRED,
                                                              parquet::Type::FIXED_LEN_BYTE_ARRAY,
                                                              parquet::ConvertedType::NONE,
                                                              static_cast<int>(byteWidth)));

        const std::shared_ptr<parquet::schema::GroupNode> schema =
            std::static_pointer_cast<parquet::schema::GroupNode>(
                parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

        parquet::WriterProperties::Builder properties;
        properties.compression(parquet::Compression::UNCOMPRESSED);

        const std::string path = _env->getConfig().getDataDir().get() + "/" + name;
        const std::shared_ptr<arrow::io::FileOutputStream> file =
            arrow::io::FileOutputStream::Open(path).ValueOrDie();
        const std::unique_ptr<parquet::ParquetFileWriter> writer =
            parquet::ParquetFileWriter::Open(file, schema, properties.build());

        parquet::RowGroupWriter* const rowGroup = writer->AppendRowGroup();
        const int64_t rowCount = static_cast<int64_t>(nodeIDs.size());

        parquet::Int64Writer* const idWriter = static_cast<parquet::Int64Writer*>(rowGroup->NextColumn());
        idWriter->WriteBatch(rowCount, nullptr, nullptr, nodeIDs.data());

        std::vector<parquet::FixedLenByteArray> embeddings(nodeIDs.size());
        for (size_t row = 0; row < nodeIDs.size(); row++) {
            embeddings[row].ptr = bytes.data() + row * byteWidth;
        }

        parquet::FixedLenByteArrayWriter* const embeddingWriter =
            static_cast<parquet::FixedLenByteArrayWriter*>(rowGroup->NextColumn());
        embeddingWriter->WriteBatch(rowCount, nullptr, nullptr, embeddings.data());

        writer->Close();

        return name;
    }

    void newChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto change = system.newChange(_graphName);
        ASSERT_TRUE(change);

        changeID = change.value()->id();
    }

    void runQuery(std::string_view query, ChangeID changeID, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void runQueryExpectingError(std::string_view query, ChangeID changeID, std::string_view reason) {
        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The whole path: the file is read, one property write per row is staged, COMMIT makes
// them readable to the change, and SUBMIT puts them at head. Only the three nodes the
// file names carry a vector; every other node reads null under the same property.
TEST_F(LoadEmbeddingV3Test, loadsOneVectorPerRowOfTheFile) {
    const std::string file = writeEmbeddingParquet("embeddings.parquet", {0, 1, 2}, loadedVectors);

    ChangeID changeID;
    newChange(changeID);

    CountSink loadSink;
    runQuery(fmt::format("LOAD EMBEDDING FROM '{}' AS emb", file), changeID, loadSink);

    ASSERT_EQ(loadSink.getCounts().size(), 1u);
    EXPECT_EQ(loadSink.getColumnNames().front(), "count");
    EXPECT_EQ(loadSink.getCounts().front(), loadedVectors.size());

    NullSink commitSink;
    runQuery("COMMIT", changeID, commitSink);

    EmbeddingSink changeSink;
    runQuery("MATCH (n) RETURN n, n.emb", changeID, changeSink);

    EXPECT_EQ(changeSink.countLoaded(), loadedVectors.size());
    expectVector(changeSink, 0, loadedVectors[0]);
    expectVector(changeSink, 1, loadedVectors[1]);
    expectVector(changeSink, 2, loadedVectors[2]);

    std::optional<std::vector<float>> unloaded;
    ASSERT_TRUE(changeSink.findValue(3, unloaded));
    EXPECT_FALSE(unloaded);

    NullSink submitSink;
    runQuery("CHANGE SUBMIT", changeID, submitSink);

    EmbeddingSink headSink;
    runQuery("MATCH (n) RETURN n, n.emb", ChangeID::head(), headSink);

    EXPECT_EQ(headSink.countLoaded(), loadedVectors.size());
    expectVector(headSink, 0, loadedVectors[0]);
    expectVector(headSink, 1, loadedVectors[1]);
    expectVector(headSink, 2, loadedVectors[2]);
}

// Writing embedding values under a property that already holds another type would produce
// a type-confused column, so the name collision is refused rather than resolved.
TEST_F(LoadEmbeddingV3Test, refusesAPropertyThatAlreadyHoldsAnotherType) {
    const std::string file = writeEmbeddingParquet("collide.parquet", {0, 1, 2}, loadedVectors);

    ChangeID changeID;
    newChange(changeID);

    runQueryExpectingError(fmt::format("LOAD EMBEDDING FROM '{}' AS name", file),
                           changeID,
                           "cannot store embeddings");
}

TEST_F(LoadEmbeddingV3Test, refusesANodeTheGraphDoesNotHave) {
    const std::string file = writeEmbeddingParquet("missing.parquet", {999}, {loadedVectors[0]});

    ChangeID changeID;
    newChange(changeID);

    runQueryExpectingError(fmt::format("LOAD EMBEDDING FROM '{}' AS emb", file),
                           changeID,
                           "does not contain node with ID 999");
}

// A file that resolves inside the data directory but is not there fails on the file the
// query named, not on the path guard.
TEST_F(LoadEmbeddingV3Test, reportsAFileThatIsNotThere) {
    ChangeID changeID;
    newChange(changeID);

    runQueryExpectingError("LOAD EMBEDDING FROM 'absent.parquet' AS emb", changeID, "absent.parquet");
}

// The statement writes, so there is nothing to stage the properties on outside a change.
TEST_F(LoadEmbeddingV3Test, refusesToWriteOutsideAChange) {
    const std::string file = writeEmbeddingParquet("head.parquet", {0, 1, 2}, loadedVectors);

    runQueryExpectingError(fmt::format("LOAD EMBEDDING FROM '{}' AS emb", file),
                           ChangeID::head(),
                           "open change");
}

// No command reads outside the data directory, so a path that climbs out of it is refused
// before the file is opened.
TEST_F(LoadEmbeddingV3Test, readsInsideTheDataDirectoryOnly) {
    ChangeID changeID;
    newChange(changeID);

    runQueryExpectingError("LOAD EMBEDDING FROM '../../embeddings.parquet' AS emb",
                           changeID,
                           "Invalid file path");
}
