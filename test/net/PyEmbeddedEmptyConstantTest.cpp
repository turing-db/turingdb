#include <gtest/gtest.h>

#include <stddef.h>
#include <memory>
#include <string>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include "EmbeddedPython.h"
#include "PyTuringEmbedded.h"
#include "TuringTest.h"

using namespace pybindings;
using namespace turing::test;

// The embedded Python client, in an embedded interpreter. A constant that holds an object
// reaches Python as a list, so the cases need no numpy.
class PyEmbeddedEmptyConstantTest : public TuringTest {
protected:
    static void SetUpTestSuite() {
        initializeEmbeddedPython();
    }

    void initialize() override {
        _embedded = std::make_unique<PyTuringEmbedded>(_outDir);
    }

    void terminate() override {
        _embedded.reset();
    }

    void expectCellRows(const std::string& query, size_t expectedRowCount) {
        const nb::dict envelope = _embedded->query(query, nb::none());

        const nb::dict data = nb::cast<nb::dict>(envelope["data"]);
        const nb::dict dtypes = nb::cast<nb::dict>(envelope["dtypes"]);
        ASSERT_EQ(nb::len(data), 1u) << query;
        EXPECT_EQ(nb::cast<std::string>(dtypes["c"]), "Cell") << query;

        const nb::list rows = nb::cast<nb::list>(data["c"]);
        ASSERT_EQ(nb::len(rows), expectedRowCount) << query;
        for (size_t row = 0; row < expectedRowCount; row++) {
            EXPECT_EQ(nb::cast<int64_t>(rows[row]), 1) << query;
        }
    }

    std::unique_ptr<PyTuringEmbedded> _embedded;
};

TEST_F(PyEmbeddedEmptyConstantTest, cellConstantOverAnEmptyUnwind) {
    expectCellRows("UNWIND [] AS x RETURN [1, 'a'][0] AS c", 0);
}

TEST_F(PyEmbeddedEmptyConstantTest, cellConstantOverAnAbsentLabel) {
    expectCellRows("MATCH (n:Tagged) RETURN [1, 'a'][0] AS c", 0);
}

TEST_F(PyEmbeddedEmptyConstantTest, cellConstantHoldingAMapOverAnAbsentLabel) {
    const nb::dict envelope = _embedded->query("MATCH (n:Tagged) RETURN [{x: 1}][0] AS c", nb::none());
    const nb::dict data = nb::cast<nb::dict>(envelope["data"]);
    const nb::list rows = nb::cast<nb::list>(data["c"]);
    EXPECT_EQ(nb::len(rows), 0u);
}

TEST_F(PyEmbeddedEmptyConstantTest, cellConstantOverRows) {
    expectCellRows("UNWIND [1, 2, 3] AS x RETURN [1, 'a'][0] AS c", 3);
}

TEST_F(PyEmbeddedEmptyConstantTest, cellConstantOverRowsThenAStepWithNoRow) {
    expectCellRows("UNWIND range(1, 100000) AS x WITH x WHERE x < 10 RETURN [1, 'a'][0] AS c", 9);
}
