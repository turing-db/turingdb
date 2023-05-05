#include "NotebookRunner.h"

#include "FileUtils.h"
#include "Command.h"

#include "BioLog.h"
#include "MsgCommon.h"
#include "MsgRun.h"

using namespace Log;

NotebookRunner::NotebookRunner(const Path& outDir, const Path& reportsDir)
    : _outDir(outDir),
    _reportsDir(reportsDir)
{
}

NotebookRunner::~NotebookRunner() {
}

void NotebookRunner::addNotebook(const Path& path) {
    _notebooks.emplace_back(std::filesystem::canonical(path));
}

bool NotebookRunner::run() {
    // Check that all notebooks exist
    bool found = true;
    for (const auto& notebook : _notebooks) {
        if (!FileUtils::exists(notebook)) {
            BioLog::log(msg::ERROR_FILE_NOT_EXISTS() << notebook.string());
            found = false;
        }
    }

    if (!found) {
        return false;
    }

    // Run each notebook
    for (const auto& notebook : _notebooks) {
        if (!runNotebook(notebook)) {
            return false;
        }
    }

    return true;
}

bool NotebookRunner::runNotebook(const Path& path) {
    if (_execNotebooks) {
        if (!executeNotebook(path)) {
            BioLog::log(msg::ERROR_FAILED_TO_RUN_NOTEBOOK() << path.string());
            return false;
        }
    }

    if (_exportHTML) {
        if (!exportNotebook(path, "html")) {
            BioLog::log(msg::ERROR_FAILED_TO_CONVERT_NOTEBOOK()
                        << path.string() << "html");
            return false;
        }
    }

    if (_exportPDF) {
        if (!exportNotebook(path, "pdf")) {
            BioLog::log(msg::ERROR_FAILED_TO_CONVERT_NOTEBOOK()
                        << path.string() << "pdf");
            return false;
        }
    }

    return true;
}

bool NotebookRunner::executeNotebook(const Path& path) {
    Command jupyterCmd("jupyter");
    jupyterCmd.addArg("nbconvert");
    jupyterCmd.addArg("--execute");
    jupyterCmd.addArg("--to");
    jupyterCmd.addArg("notebook");
    jupyterCmd.addArg("--inplace");
    jupyterCmd.addArg(path.string());
    jupyterCmd.setWorkingDir(_outDir);
    jupyterCmd.setScriptPath(_outDir/"jupyter_exec.sh");

    const auto logFile = _outDir/"jupyter_exec.log";
    jupyterCmd.setLogFile(logFile);

    jupyterCmd.setWriteOnStdout(!_silent);

    BioLog::log(msg::INFO_RUNNING_NOTEBOOK() << path.string());
    BioLog::log(msg::INFO_JUPYTER_LOG_FILE() << logFile.string());

    if (!jupyterCmd.run()) {
        return false;
    }

    return jupyterCmd.getReturnCode() == 0;
}

bool NotebookRunner::exportNotebook(const Path& path, const std::string& toDest) {
    Command jupyterCmd("jupyter");
    jupyterCmd.addArg("nbconvert");
    jupyterCmd.addArg("--output-dir");
    jupyterCmd.addArg(toDest);
    jupyterCmd.addArg("--to");
    jupyterCmd.addArg(toDest);
    jupyterCmd.addArg(path.string());

    jupyterCmd.setWorkingDir(_outDir);
    jupyterCmd.setScriptPath(_outDir/("jupyter_convert_"+toDest+".sh"));
    jupyterCmd.setLogFile(_outDir/("jupyter_convert_"+toDest+".log"));

    BioLog::log(msg::INFO_CONVERTING_NOTEBOOK() << path.string() << toDest);

    const std::string notebookName = path.stem().string();
    const auto reportPath = _reportsDir/(notebookName+"."+toDest);
    BioLog::log(msg::INFO_NOTEBOOK_REPORT()
                << reportPath.string());

    if (!jupyterCmd.run()) {
        return false;
    }

    return jupyterCmd.getReturnCode() == 0;
}
