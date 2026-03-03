#pragma once

#include <shared_mutex>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "ExtensionDescriptor.h"
#include "ExtensionInterface.h"
#include "Path.h"

namespace db {

class ProcedureManager;

class ExtensionManager {
public:
    using Extensions = std::vector<ExtensionDescriptor*>;

    ExtensionManager(const fs::Path& userExtensionsDir,
                     const fs::Path& installExtensionsDir,
                     ProcedureManager* procedures);
    ~ExtensionManager();

    void installExtension(std::string_view name);
    bool isInstalled(std::string_view name) const;
    void getInstalled(Extensions& result) const;

private:
    mutable std::shared_mutex _mutex;
    fs::Path _userExtensionsDir;
    fs::Path _installExtensionsDir;
    ProcedureManager* _procedures {nullptr};
    Extensions _installed;
    std::unordered_map<std::string_view, ExtensionDescriptor*> _installedMap;

    void loadExtensionDef(const TuringExtensionDef* def,
                          ExtensionDescriptor::Handle handle);
};

}
