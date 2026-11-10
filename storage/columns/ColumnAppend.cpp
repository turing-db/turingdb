#include "ColumnAppend.h"

#include <vector>

#include "AllowedKinds.h"
#include "ColumnOperatorDispatcher.h"

using namespace db;

void db::appendColumnRows(const Column* source, Column* destination, size_t offset, size_t rowCount) {
    const auto appendRows = [destination, offset, rowCount]<typename SourceColumn>(const SourceColumn* typedSource) {
        using Row = typename SourceColumn::ValueType;

        SourceColumn* typedDestination = static_cast<SourceColumn*>(destination);
        const std::vector<Row>& sourceRows = typedSource->getRaw();
        std::vector<Row>& destinationRows = typedDestination->getRaw();

        destinationRows.insert(destinationRows.end(), sourceRows.begin() + offset, sourceRows.begin() + offset + rowCount);
    };

    using ExcludedNonVector = ExcludedContainers<
        ContainerKind::code<ColumnConst>(),
        ContainerKind::code<ColumnSet>()>;
    using Dispatcher = ColumnSingleDispatcher<OutputtedTypes::Allowed,
                                              decltype(appendRows),
                                              ExcludedNonVector>;

    Dispatcher::dispatch(source, appendRows);
}
