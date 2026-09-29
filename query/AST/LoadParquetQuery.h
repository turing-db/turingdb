#pragma once

#include <string_view>

#include "QueryCommand.h"

#include "DurationSpec.h"

#include "Path.h"

namespace db {

class CypherAST;
class DeclContext;

class LoadParquetQuery : public QueryCommand {
public:
    static LoadParquetQuery* create(CypherAST* ast,
                                    fs::Path&& filePath);

    Kind getKind() const override { return Kind::LOAD_PARQUET_QUERY; }

    std::string_view getGraphName() const { return _graphName; }

    void setGraphName(std::string_view name) { _graphName = name; }
    void setDurationSpecs(DurationSpec&& specs) { _durationSpecs = std::move(specs); }

    const fs::Path& getFilePath() const { return _filePath; }
    const DurationSpec& getDurationSpecs() const { return _durationSpecs; }

private:
    fs::Path _filePath;
    std::string_view _graphName;
    DurationSpec _durationSpecs;

    LoadParquetQuery(DeclContext* declContext,
                     fs::Path&& filePath);
    ~LoadParquetQuery() override;
};

}
