#pragma once

#include <memory>
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
    ExtensionManager(const fs::Path& userExtensionsDir,
                     const fs::Path& installExtensionsDir,
                     ProcedureManager* procedures);
    ~ExtensionManager();

    void installExtension(std::string_view name);
    bool isInstalled(std::string_view name) const;

    const std::vector<std::unique_ptr<ExtensionDescriptor>>& installed() const {
        return _installed;
    }

private:
    fs::Path _userExtensionsDir;
    fs::Path _installExtensionsDir;
    ProcedureManager* _procedures {nullptr};
    std::vector<std::unique_ptr<ExtensionDescriptor>> _installed;
    std::unordered_map<std::string_view, ExtensionDescriptor*> _installedMap;

    void loadExtensionDef(const TuringExtensionDef* def,
                          ExtensionDescriptor::Handle handle);
};

}
