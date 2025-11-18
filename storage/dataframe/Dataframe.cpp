#include "Dataframe.h"

#include <sstream>

#include "NamedColumn.h"
#include "columns/Column.h"
#include "columns/ColumnDispatcher.h"

#include "FatalException.h"

using namespace db;

Dataframe::Dataframe() = default;

Dataframe::~Dataframe() = default;

void Dataframe::addColumn(NamedColumn* column) {
    _cols.push_back(column);
    _tagToColumnMap.insert(column->getTag().getValue(), column);
}

size_t Dataframe::getRowCount() const {
    if (_cols.empty()) {
        return 0;
    }

    return _cols[0]->getColumn()->size();
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
        out << "Column $" << col->getTag().getValue() << "\t";
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
    if (this->size() != other->size()) {
        throw FatalException(fmt::format(
            "Attempted to append a Dataframe of size {} to a Dataframe of size {}.",
            other->size(), this->size()));
    }

    const size_t oldRows = this->getRowCount();
    const size_t addRows = other->getRowCount();
    const size_t newRows = oldRows + addRows;

    const NamedColumns& otherCols = other->cols();

    for (size_t i = 0; i < _cols.size(); i++) {
        NamedColumn* dstNamed = _cols[i];
        NamedColumn* srcNamed = otherCols[i];

        Column* dst = dstNamed->getColumn();
        Column* src = srcNamed->getColumn();

        // Resize destination to fit new data
        dispatchColumnVector(dst, [&](auto* dstColumnVector) {
            auto* srcColumnVector = dynamic_cast<decltype(dstColumnVector)>(src);
            if (!srcColumnVector) {
                throw FatalException(
                    fmt::format("Attempted to append to a Dataframe whose column at "
                                "index {} is not a ColumnVector<T>.",
                                i));
            }

            dstColumnVector->resize(newRows);
            auto& srcRaw = srcColumnVector->getRaw();
            auto& dstRaw = dstColumnVector->getRaw();

            std::copy(begin(srcRaw),end(srcRaw),begin(dstRaw) + oldRows);
        });
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
