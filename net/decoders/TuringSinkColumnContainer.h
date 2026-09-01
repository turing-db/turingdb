#pragma once

#include <stddef.h>
#include <string_view>

namespace db {
class Column;
class Dataframe;
class DataframeManager;
}

namespace net::proto {

// Column-container view over a db::Dataframe for the decode sink. The turing proto decoder
// works against containers that reference columns directly, so this adapter hides the
// NamedColumn indirection we use in the Dataframe class.
class TuringSinkColumnContainer {
public:
    TuringSinkColumnContainer(db::Dataframe* dataframe, db::DataframeManager* dataframeManager);
    ~TuringSinkColumnContainer();

    size_t size() const;

    db::Column* operator[](size_t index);

    void addColumn(db::Column* column, std::string_view name);

private:
    db::Dataframe* _dataframe {nullptr};
    db::DataframeManager* _dataframeManager {nullptr};
};

}
