#include "ExtensionManager.h"

#include <dlfcn.h>

#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>

#include "ExtensionDescriptor.h"
#include "procedures/ProcedureManager.h"
#include "procedures/ProcedureNamespace.h"

#include "TuringException.h"

using namespace db;

ExtensionManager::ExtensionManager(const fs::Path& userExtensionsDir,
                                   const fs::Path& installExtensionsDir,
                                   ProcedureManager* procedures)
    : _userExtensionsDir(userExtensionsDir),
    _installExtensionsDir(installExtensionsDir),
    _procedures(procedures)
{
}

ExtensionManager::~ExtensionManager() {
    for (ExtensionDescriptor* ext : _installed) {
        ExtensionDescriptor::Handle handle = ext->getHandle();
        if (handle) {
            dlclose(handle);
        }
        delete ext;
    }
}

std::unique_ptr<ExtensionManager> ExtensionManager::create(const fs::Path& userExtensionsDir,
                                                           const fs::Path& installExtensionsDir,
                                                           ProcedureManager* procedures) {
    return std::unique_ptr<ExtensionManager>(new ExtensionManager(userExtensionsDir, installExtensionsDir, procedures));
}

void ExtensionManager::installExtension(std::string_view name) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    const auto it = _installedMap.find(name);
    if (it != _installedMap.end()) {
        throw TuringException(
            fmt::format("Extension '{}' is already installed",
                        name));
    }

    for (char c : name) {
        if (!(isalnum(c) || c == '_')) {
            throw TuringException(
                fmt::format("Extension name contains invalid character '{}': '{}'",
                            c, name));
        }
    }

#ifdef __APPLE__
    const std::string extFilename = std::string(name) + ".dylib";
#else
    const std::string extFilename = std::string(name) + ".so";
#endif

    // Try install directory first, then user extensions directory
    fs::Path extPath = _installExtensionsDir / extFilename;
    void* handle = dlopen(extPath.c_str(), RTLD_NOW | RTLD_LOCAL);

    if (!handle && !_userExtensionsDir.empty()) {
        extPath = _userExtensionsDir / extFilename;
        handle = dlopen(extPath.c_str(), RTLD_NOW | RTLD_LOCAL);
    }

    if (!handle) {
        throw TuringException(
            fmt::format("Could not load extension '{}': {}",
                        name, dlerror()));
    }

    using ExtensionEntryFn = const TuringExtensionDef* (*)();

    void* sym = dlsym(handle, "turing_extension");
    if (!sym) {
        dlclose(handle);
        throw TuringException(
            fmt::format("Extension '{}' does not export 'turing_extension': {}",
                        name, dlerror()));
    }

    auto entryFn = reinterpret_cast<ExtensionEntryFn>(sym);
    const TuringExtensionDef* def = entryFn();
    if (!def) {
        dlclose(handle);
        throw TuringException(
            fmt::format("Extension '{}' returned null definition",
                        name));
    }

    loadExtensionDef(def, handle);
}

bool ExtensionManager::isInstalled(std::string_view name) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _installedMap.find(name) != _installedMap.end();
}

void ExtensionManager::getInstalled(Extensions& result) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    result = _installed;
}

void ExtensionManager::loadExtensionDef(const TuringExtensionDef* def,
                                        ExtensionDescriptor::Handle handle) {
    const char* nsName = def->_namespaceName;

    spdlog::info("Loading extension '{}'", nsName);

    ProcedureNamespace* ns = _procedures->getNamespace(nsName);
    if (ns) {
        throw TuringException(
            fmt::format("Procedure namespace '{}' already exists",
                        nsName));
    }
    ns = _procedures->createNamespace(nsName);
    def->_initCallback(ns);

    ExtensionDescriptor* descriptor = new ExtensionDescriptor(nsName, handle);
    _installed.push_back(descriptor);
    _installedMap[descriptor->getName()] = descriptor;
}
