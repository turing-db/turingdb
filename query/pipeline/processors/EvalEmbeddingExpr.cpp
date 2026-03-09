#include "EvalEmbeddingExpr.h"

#include <algorithm>
#include <math.h>
#include <span>

#include <spdlog/fmt/fmt.h>

#include "columns/Column.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnEmbeddingConst.h"
#include "columns/ColumnEmbeddingMany.h"

#include "FatalException.h"
#include "PipelineException.h"

namespace db {

static size_t getRowCount(const Column* lhs, const Column* rhs) {
    return std::max(lhs->size(), rhs->size());
}

struct ResolvedEmbeddingColumn {
    const ColumnEmbeddingMany* many {nullptr};
    const ColumnEmbeddingConst* cnst {nullptr};

    std::span<const float> spanAt(size_t row) const {
        return many ? (*many)[row] : cnst->at(0);
    }

    uint32_t dimension() const {
        return many ? many->dimension()
                    : static_cast<uint32_t>(cnst->at(0).size());
    }
};

static ResolvedEmbeddingColumn resolveColumn(const Column* col, const char* context) {
    ResolvedEmbeddingColumn resolved;
    resolved.many = dynamic_cast<const ColumnEmbeddingMany*>(col);
    if (!resolved.many) {
        resolved.cnst = dynamic_cast<const ColumnEmbeddingConst*>(col);
    }
    if (!resolved.many && !resolved.cnst) {
        throw FatalException(
            fmt::format("EvalEmbeddingExpr::{}: column is not an embedding column", context));
    }
    return resolved;
}

void EvalEmbeddingExpr::evalEqual(Column* res, const Column* lhs, const Column* rhs) {
    const auto lhsCol = resolveColumn(lhs, "evalEqual");
    const auto rhsCol = resolveColumn(rhs, "evalEqual");

    const uint32_t dimL = lhsCol.dimension();
    const uint32_t dimR = rhsCol.dimension();
    if (dimL != dimR) {
        throw PipelineException(
            fmt::format("Embedding dimension mismatch: {} vs {}", dimL, dimR));
    }

    auto* out = static_cast<ColumnMask*>(res);
    const size_t rowCount = getRowCount(lhs, rhs);
    out->clear();
    out->reserve(rowCount);

    for (size_t i = 0; i < rowCount; i++) {
        const auto spanL = lhsCol.spanAt(i);
        const auto spanR = rhsCol.spanAt(i);

        bool equal = true;
        for (uint32_t j = 0; j < dimL; j++) {
            if (spanL[j] != spanR[j]) {
                equal = false;
                break;
            }
        }
        out->push_back(equal);
    }
}

void EvalEmbeddingExpr::evalNotEqual(Column* res, const Column* lhs, const Column* rhs) {
    const auto lhsCol = resolveColumn(lhs, "evalNotEqual");
    const auto rhsCol = resolveColumn(rhs, "evalNotEqual");

    const uint32_t dimL = lhsCol.dimension();
    const uint32_t dimR = rhsCol.dimension();
    if (dimL != dimR) {
        throw PipelineException(
            fmt::format("Embedding dimension mismatch: {} vs {}", dimL, dimR));
    }

    auto* out = static_cast<ColumnMask*>(res);
    const size_t rowCount = getRowCount(lhs, rhs);
    out->clear();
    out->reserve(rowCount);

    for (size_t i = 0; i < rowCount; i++) {
        const auto spanL = lhsCol.spanAt(i);
        const auto spanR = rhsCol.spanAt(i);

        bool equal = true;
        for (uint32_t j = 0; j < dimL; j++) {
            if (spanL[j] != spanR[j]) {
                equal = false;
                break;
            }
        }
        out->push_back(!equal);
    }
}

void EvalEmbeddingExpr::evalCosineSimilarity(Column* res, const Column* lhs, const Column* rhs) {
    const auto lhsCol = resolveColumn(lhs, "evalCosineSimilarity");
    const auto rhsCol = resolveColumn(rhs, "evalCosineSimilarity");

    const uint32_t dimL = lhsCol.dimension();
    const uint32_t dimR = rhsCol.dimension();
    if (dimL != dimR) {
        throw PipelineException(
            fmt::format("Embedding dimension mismatch in cosineSimilarity: {} vs {}", dimL, dimR));
    }

    auto* out = static_cast<ColumnVector<double>*>(res);
    const size_t rowCount = getRowCount(lhs, rhs);
    out->clear();
    out->reserve(rowCount);

    for (size_t i = 0; i < rowCount; i++) {
        const auto spanL = lhsCol.spanAt(i);
        const auto spanR = rhsCol.spanAt(i);

        double dot = 0.0;
        double normL = 0.0;
        double normR = 0.0;

        for (uint32_t j = 0; j < dimL; j++) {
            dot += static_cast<double>(spanL[j]) * static_cast<double>(spanR[j]);
            normL += static_cast<double>(spanL[j]) * static_cast<double>(spanL[j]);
            normR += static_cast<double>(spanR[j]) * static_cast<double>(spanR[j]);
        }

        const double denom = sqrt(normL) * sqrt(normR);
        const double similarity = (denom > 0.0) ? (dot / denom) : 0.0;
        out->push_back(similarity);
    }
}

}
