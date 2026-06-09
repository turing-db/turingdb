#include <gtest/gtest.h>

#include <stdint.h>

#include <string>
#include <vector>

#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "QueryConfig.h"
#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
#include "SystemAccessor.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

// End-to-end LOAD EMBEDDING tests. The Parquet inputs are produced here with the
// bundled Parquet writer, then loaded through the full query path so the graph
// property-type checks in LoadEmbeddingProcessor are exercised.
class LoadEmbeddingTest : public TuringTest {
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        _db = &_env->getDB();
    }

protected:
    std::string _graphName = "embeddingloaddb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    ChangeID _currentChange {ChangeID::head()};
    QueryConfig _queryConfig;

    static constexpr auto emptyCallback = [](const Dataframe*) -> void {};

    auto query(std::string_view query, auto callback) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks,
                               CommitHash::head(), _currentChange);
        return _db->query(query, state);
    }

    void newChange() {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);
        _currentChange = res.value()->id();
    }

    void submitCurrentChange() {
        auto res = query("CHANGE SUBMIT", emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        _currentChange = ChangeID::head();
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }

    // Writes a valid embedding Parquet (INT64 node_id + FIXED_LEN_BYTE_ARRAY
    // embedding) into the graph's data directory and returns the bare file name
    // to use in the LOAD EMBEDDING query.
    std::string writeEmbeddingParquet(const std::string& name,
                                      const std::vector<int64_t>& ids,
                                      const std::vector<std::vector<float>>& vectors) {
        const size_t dimension = vectors.front().size();
        const size_t byteWidth = dimension * sizeof(float);

        std::vector<uint8_t> bytes;
        for (const auto& vector : vectors) {
            const auto* raw = reinterpret_cast<const uint8_t*>(vector.data());
            bytes.insert(bytes.end(), raw, raw + vector.size() * sizeof(float));
        }

        parquet::schema::NodeVector fields;
        fields.push_back(parquet::schema::PrimitiveNode::Make(
            "node_id", parquet::Repetition::REQUIRED, parquet::Type::INT64));
        fields.push_back(parquet::schema::PrimitiveNode::Make(
            "embedding", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            parquet::ConvertedType::NONE, static_cast<int>(byteWidth)));

        const auto schemaNode = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make(
                "schema", parquet::Repetition::REQUIRED, fields));

        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::UNCOMPRESSED);

        const std::string path = _env->getConfig().getDataDir().get() + "/" + name;
        const auto outFile = arrow::io::FileOutputStream::Open(path).ValueOrDie();
        auto writer = parquet::ParquetFileWriter::Open(outFile, schemaNode, builder.build());

        parquet::RowGroupWriter* rowGroup = writer->AppendRowGroup();

        auto* idWriter = static_cast<parquet::Int64Writer*>(rowGroup->NextColumn());
        idWriter->WriteBatch(static_cast<int64_t>(ids.size()), nullptr, nullptr, ids.data());

        std::vector<parquet::FixedLenByteArray> embeddingValues(ids.size());
        for (size_t row = 0; row < ids.size(); ++row) {
            embeddingValues[row].ptr = bytes.data() + row * byteWidth;
        }
        auto* embeddingWriter =
            static_cast<parquet::FixedLenByteArrayWriter*>(rowGroup->NextColumn());
        embeddingWriter->WriteBatch(static_cast<int64_t>(ids.size()), nullptr, nullptr,
                                    embeddingValues.data());

        writer->Close();
        return name;
    }

    // Creates nodes (:Vec {name}) in a fresh graph. Node IDs are assigned in
    // creation order, so the returned graph has node IDs 0..names.size()-1.
    void createNamedNodes(const std::vector<std::string>& names) {
        newChange();
        for (const auto& name : names) {
            auto res = query(fmt::format("CREATE (n:Vec {{name: \"{}\"}})", name), emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
        submitCurrentChange();
    }
};

TEST_F(LoadEmbeddingTest, loadsEmbeddingsIntoNewProperty) {
    createNamedNodes({"a", "b", "c"});

    const std::vector<std::vector<float>> vectors {
        {0.1f, 0.2f, 0.3f, 0.4f},
        {0.5f, 0.6f, 0.7f, 0.8f},
        {0.9f, 1.0f, 1.1f, 1.2f},
    };
    const std::string file = writeEmbeddingParquet("ok.parquet", {0, 1, 2}, vectors);

    {
        newChange();
        auto res = query(fmt::format("LOAD EMBEDDING FROM '{}' AS emb", file), emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        submitCurrentChange();
    }

    std::vector<std::vector<float>> actual(3);
    auto res = query("MATCH (n:Vec) RETURN n.name, n.emb", [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
        const auto* vecs = findColumn(df, "n.emb")->as<ColumnOptVector<types::Embedding::Primitive>>();
        ASSERT_TRUE(names);
        ASSERT_TRUE(vecs);

        for (size_t row = 0; row < df->getLogicalRowCount(); ++row) {
            ASSERT_TRUE(names->at(row));
            ASSERT_TRUE(vecs->at(row));
            const size_t index = static_cast<size_t>((*names->at(row))[0] - 'a');
            const auto& emb = *vecs->at(row);
            actual[index].assign(emb.begin(), emb.end());
        }
    });
    ASSERT_TRUE(res) << res.getError();

    for (size_t i = 0; i < vectors.size(); ++i) {
        ASSERT_EQ(actual[i].size(), vectors[i].size());
        for (size_t j = 0; j < vectors[i].size(); ++j) {
            EXPECT_FLOAT_EQ(actual[i][j], vectors[i][j]);
        }
    }
}

TEST_F(LoadEmbeddingTest, loadIntoExistingNonEmbeddingPropertyFails) {
    // 'name' already exists as a String property; loading embeddings into it must
    // fail cleanly rather than corrupt the column (previously a crash on submit).
    createNamedNodes({"a", "b", "c"});

    const std::vector<std::vector<float>> vectors {
        {0.1f, 0.2f, 0.3f, 0.4f},
        {0.5f, 0.6f, 0.7f, 0.8f},
        {0.9f, 1.0f, 1.1f, 1.2f},
    };
    const std::string file = writeEmbeddingParquet("collide.parquet", {0, 1, 2}, vectors);

    newChange();
    auto res = query(fmt::format("LOAD EMBEDDING FROM '{}' AS name", file), emptyCallback);
    ASSERT_FALSE(res) << "expected LOAD EMBEDDING into a String property to fail";
}

TEST_F(LoadEmbeddingTest, loadForMissingNodeFails) {
    createNamedNodes({"a", "b", "c"});

    // node_id 99 does not exist in the graph.
    const std::vector<std::vector<float>> vectors {{0.1f, 0.2f, 0.3f, 0.4f}};
    const std::string file = writeEmbeddingParquet("missing.parquet", {99}, vectors);

    newChange();
    auto res = query(fmt::format("LOAD EMBEDDING FROM '{}' AS emb", file), emptyCallback);
    ASSERT_FALSE(res) << "expected LOAD EMBEDDING for a non-existent node to fail";
}

TEST_F(LoadEmbeddingTest, loadFromMissingFileFails) {
    createNamedNodes({"a"});

    newChange();
    auto res = query("LOAD EMBEDDING FROM 'does-not-exist.parquet' AS emb", emptyCallback);
    ASSERT_FALSE(res) << "expected LOAD EMBEDDING from a missing file to fail";
}
