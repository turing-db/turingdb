#pragma once

#include <string_view>

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class InstallExtensionQuery : public QueryCommand {
public:
    static InstallExtensionQuery* create(CypherAST* ast, std::string_view name);

    Kind getKind() const override { return Kind::INSTALL_EXTENSION_QUERY; }
    std::string_view getExtensionName() const { return _extensionName; }

private:
    std::string_view _extensionName;

    InstallExtensionQuery(DeclContext* declContext, std::string_view name);
    ~InstallExtensionQuery() override;
};

}
