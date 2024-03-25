#include "NotebookRunner.h"

#include "FileUtils.h"
#include "Command.h"

#include "BioLog.h"
#include "PerfStat.h"
#include "TimerStat.h"
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
    
    if (_generateReport) {
        if (!generateReport(path)) {
            BioLog::log(msg::ERROR_FAILED_TO_GENERATE_REPORT()
                    << path.string());
            return false;
        }
    }

    return true;
}

bool NotebookRunner::generateReport(const Path& path) {
    TimerStat timer("Generating report for " + path.string());
    
    const char* turingHome = getenv("TURING_HOME");
    if (!turingHome) {
        BioLog::log(msg::ERROR_INCORRECT_ENV_SETUP());
        return false;
    }

    const FileUtils::Path scriptPath = FileUtils::Path(turingHome)
                                     / "scripts"
                                     / "latex_template"
                                     / "report_generation_scripts"
                                     / "run_latex_template.py";

    Command reportCmd("python3");
    reportCmd.addArg(scriptPath.string());
    reportCmd.addArg(path.string());
    reportCmd.addArg("-ro");
    reportCmd.addArg(_outDir/"pdf_reports"); //reportCmd.addArg(_reportsDir.string());
    reportCmd.addArg("-so");
    reportCmd.addArg(_outDir.string());

    for (const auto& envVar : _envVars) {
        reportCmd.setEnvVar(envVar.argName, envVar.argValue);
    }
    
    reportCmd.setScriptPath(_outDir/"generate_report.sh");

    const auto logFile = _outDir / "generate_report.log";
    reportCmd.setLogFile(logFile);
    reportCmd.setWriteOnStdout(!_silent);

    if (!reportCmd.run()) {
        return false;
    }

    return reportCmd.getReturnCode() == 0;
}

bool NotebookRunner::executeNotebook(const Path& path) {
    TimerStat timer("Executing " + path.string());
    Command jupyterCmd("jupyter");
    jupyterCmd.addArg("nbconvert");
    jupyterCmd.addArg("--execute");
    jupyterCmd.addArg("--to");
    jupyterCmd.addArg("notebook");
    jupyterCmd.addArg("--inplace");
    jupyterCmd.addArg(path.string());
    jupyterCmd.setWorkingDir(_outDir);
    jupyterCmd.setScriptPath(_outDir/"jupyter_exec.sh");

    for (const auto& envVar : _envVars) {
        jupyterCmd.setEnvVar(envVar.argName, envVar.argValue);
    }

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
    TimerStat timer("Exporting " + path.string());
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

    for (const auto& envVar : _envVars) {
        jupyterCmd.setEnvVar(envVar.argName, envVar.argValue);
    }

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
