#include "TuringSinkColumnContainer.h"

#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"

using namespace net::proto;

TuringSinkColumnContainer::TuringSinkColumnContainer(db::Dataframe* dataframe, db::DataframeManager* dataframeManager)
    : _dataframe(dataframe),
    _dataframeManager(dataframeManager)
{
}

TuringSinkColumnContainer::~TuringSinkColumnContainer() {
}

size_t TuringSinkColumnContainer::size() const {
    return _dataframe->size();
}

db::Column* TuringSinkColumnContainer::operator[](size_t index) {
    return _dataframe->cols()[index]->getColumn();
}

void TuringSinkColumnContainer::addColumn(db::Column* column, std::string_view name) {
    db::NamedColumn* namedColumn = db::NamedColumn::create(_dataframeManager, column, _dataframeManager->allocTag());

    namedColumn->rename(name);
    _dataframe->addColumn(namedColumn);
}
