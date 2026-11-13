#include "ColumnAppend.h"

#include "AllowedKinds.h"
#include "ColumnOperatorDispatcher.h"

using namespace db;

void db::appendColumnRows(const Column* source, Column* destination, size_t offset, size_t rowCount) {
    const auto appendTypedRows = [destination, offset, rowCount]<typename SourceColumn>(const SourceColumn* typedSource) {
        SourceColumn* typedDestination = static_cast<SourceColumn*>(destination);
        typedDestination->appendRows(*typedSource, offset, rowCount);
    };

    using ExcludedNonVector = ExcludedContainers<
        ContainerKind::code<ColumnConst>(),
        ContainerKind::code<ColumnSet>()>;
    using Dispatcher = ColumnSingleDispatcher<OutputtedTypes::Allowed,
                                              decltype(appendTypedRows),
                                              ExcludedNonVector>;

    Dispatcher::dispatch(source, appendTypedRows);
}
