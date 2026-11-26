#include "EmbeddedPython.h"

#include <Python.h>

void turing::test::initializeEmbeddedPython() {
    if (Py_IsInitialized()) {
        return;
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
