#include "EmbeddedPython.h"

#include <Python.h>

void turing::test::initializeEmbeddedPython() {
    if (Py_IsInitialized()) {
        return;
    }

    // pymalloc keeps objects in mmap'd arenas, which LeakSanitizer does not scan, so every
    // block referenced only from a Python object would read as a leak.
    PyPreConfig preConfig;
    PyPreConfig_InitIsolatedConfig(&preConfig);
    preConfig.allocator = PYMEM_ALLOCATOR_MALLOC;

    const PyStatus preInitStatus = Py_PreInitialize(&preConfig);
    if (PyStatus_Exception(preInitStatus)) {
        Py_ExitStatusException(preInitStatus);
    }

    PyConfig config;
    PyConfig_InitIsolatedConfig(&config);

    const PyStatus homeStatus = PyConfig_SetBytesString(&config, &config.home, PYTHON_HOME);
    if (PyStatus_Exception(homeStatus)) {
        Py_ExitStatusException(homeStatus);
    }

    const PyStatus initStatus = Py_InitializeFromConfig(&config);
    PyConfig_Clear(&config);
    if (PyStatus_Exception(initStatus)) {
        Py_ExitStatusException(initStatus);
    }
}
