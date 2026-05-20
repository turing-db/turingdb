#include "Dataframe.h"

#include <algorithm>
#include <sstream>

#include "NamedColumn.h"
#include "columns/AllowedKinds.h"
#include "columns/Column.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"

#include "FatalException.h"
#include "columns/ContainerKind.h"

using namespace db;

Dataframe::Dataframe() = default;

Dataframe::~Dataframe() = default;

void Dataframe::clear() {
    for (const NamedColumn* ncols : _cols) {
        ncols->getColumn()->clear();
    }
}

void Dataframe::addColumn(NamedColumn* column) {
    _cols.push_back(column);
    _tagToColumnMap.insert(column->getTag().getValue(), column);
}

size_t Dataframe::getLogicalRowCount() const {
    size_t maxSize = 0;
    for (const NamedColumn* ncol : _cols) {
        const Column* col = ncol->getColumn();
        maxSize = std::max(maxSize, col->size());
    }
    return maxSize;
}

bool Dataframe::isRectangular() const {
    constexpr ContainerKind::Code colConstKind =
        ColumnKind::extractContainerKind(ColumnConst<int>::staticKind());

    std::optional<size_t> expectedRowCount(std::nullopt);

    for (const NamedColumn* ncol : _cols) {
        bioassert(ncol && ncol->getColumn(),
                  "Attempted to check shape of dataframe with null column.");

        const Column* col = ncol->getColumn();
        const ContainerKind::Code containerKind = col->getContainerKind();

        // ColumnConst can be any size, ignore it
        if (containerKind == colConstKind) {
            continue;
        }

        // All other columns must be the same size
        const size_t actualSize = col->size();
        // If we haven't encountered a non-ColumnConst yet, set the expected row
        // count to be the size of this column
        if (!expectedRowCount.has_value()) {
            expectedRowCount = actualSize;
        }
        // All sized columns should be the same size as the expected size
        if (actualSize != expectedRowCount) {
            return false;
        }
    }

    return true;
}

void Dataframe::dump(std::ostream& out) const {
    if (_cols.empty()) {
        return;
    }

    std::vector<std::ostringstream> colStreams(_cols.size());
    for (size_t i = 0; i < _cols.size(); ++i) {
        const NamedColumn* col = _cols[i];
        col->getColumn()->dump(colStreams[i]);
    }

    std::vector<std::vector<std::string>> colLines;
    colLines.reserve(_cols.size());
    size_t maxLines = 0;

    for (auto& ss : colStreams) {
        std::vector<std::string> lines;
        std::string line;
        std::istringstream in(ss.str());
        while (std::getline(in, line)) {
            lines.push_back(line);
        }
        maxLines = std::max(maxLines, lines.size());
        colLines.push_back(std::move(lines));
    }

    for (const NamedColumn* col : _cols) {
        out << "Column $" << col->getTag().getValue() << '[' << col->getName() << ']'
            << "\t";
    }
    out << '\n';

    for (size_t row = 0; row < maxLines; ++row) {
        for (size_t c = 0; c < _cols.size(); ++c) {
            const auto& lines = colLines[c];
            if (row < lines.size()) {
                out << lines[row];
            } else {
                out << "_";
            }
            out << "\t";
        }
        out << "\n";
    }
}

void Dataframe::copyFromLine(const Dataframe* other, size_t startRow, size_t rowCount) {
    const NamedColumns& otherColumns = other->cols();
    const size_t copySize = std::min(_cols.size(), otherColumns.size());
    for (size_t i = 0; i < copySize; i++) {
        Column* otherColumn = otherColumns[i]->getColumn();
        Column* column = _cols[i]->getColumn();
        column->assignFromLine(otherColumn, startRow, rowCount);
    }
}

void Dataframe::append(const Dataframe* other) {
    bioassert(this->size() == other->size(),
              "Attempted to append a Dataframe of size {} to a Dataframe of size {}.",
              other->size(), this->size());

    const size_t oldRows = this->getLogicalRowCount();
    const size_t addRows = other->getLogicalRowCount();
    const size_t newRows = oldRows + addRows;

    const NamedColumns& otherCols = other->cols();

    for (size_t i = 0; i < _cols.size(); i++) {
        Column* dst = _cols[i]->getColumn();
        const Column* src = otherCols[i]->getColumn();

        const auto appendCol = [dst, oldRows, newRows]
                         <template <typename> class Container, typename T>
                         (const Container<T>* srcCol) {
            auto* dstVec = dynamic_cast<ColumnVector<T>*>(dst);
            bioassert(dstVec, "Invalid destination vector to append.");

            dstVec->resize(newRows);
            auto& dstRaw = dstVec->getRaw();

            if constexpr (std::is_same_v<Container<T>, ColumnConst<T>>) {
                auto start = begin(dstRaw) + oldRows;
                const T& value = srcCol->getRaw();
                std::fill(start, end(dstRaw), value);
            } else {
                const std::vector<T>& srcRaw = srcCol->getRaw();
                auto fromIt = begin(dstRaw) + oldRows;
                std::ranges::copy(srcRaw, fromIt);
            }
        };

        using Types = CartesianProductKinds;
        using Dispatcher = ColumnSingleDispatcher<Types::Allowed, decltype(appendCol), Types::Excluded>;
        Dispatcher::dispatch(src, appendCol);
    }
}

void Dataframe::copyFrom(const Dataframe* other) {
    const NamedColumns& otherColumns = other->cols();
    const size_t copySize = std::min(_cols.size(), otherColumns.size());
    for (size_t i = 0; i < copySize; i++) {
        Column* otherColumn = otherColumns[i]->getColumn();
        Column* column = _cols[i]->getColumn();
        column->assign(otherColumn);
    }
}

bool Dataframe::hasSameShape(const Dataframe* other) const {
    const size_t colCount = _cols.size();
    if (colCount != other->size()) {
        return false;
    }

    const auto& otherCols = other->cols();
    for (size_t i = 0; i < colCount; i++) {
        const Column* col = _cols[i]->getColumn();
        const Column* otherCol = otherCols[i]->getColumn();
        if (col->getKind() != otherCol->getKind()) {
            return false;
        }
    }

    return true;
}
