#include "NotebookRunner.h"

#include <spdlog/spdlog.h>

#include "FileUtils.h"
#include "Command.h"

#include "PerfStat.h"
#include "TimerStat.h"
#include "LogUtils.h"

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
            logt::FileNotFound(notebook.string());
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
            spdlog::error("Failed to run notebook {}", path.string());
            return false;
        }
    }

    if (_exportHTML) {
        if (!exportNotebook(path, "html")) {
            spdlog::error("Failed to convert notebook {} to {}", path.string(), "html");
            return false;
        }
    }

    if (_exportPDF) {
        if (!exportNotebook(path, "pdf")) {
            spdlog::error("Failed to convert notebook {} to {}", path.string(), "pdf");
            return false;
        }
    }
    
    if (_generateReport) {
        if (!generateReport(path)) {
            spdlog::error("Failed to generate report for notebook {}", path.string());
            return false;
        }
    }

    return true;
}

bool NotebookRunner::generateReport(const Path& path) {
    TimerStat timer("Generating report for " + path.string());
    
    const char* turingHome = getenv("TURING_HOME");
    if (!turingHome) {
        logt::TuringHomeUndefined();
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

    spdlog::info("Running notebook {}", path.string());
    spdlog::info("Jupyter log written in {}", logFile.string());

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

    spdlog::info("Converting notebook {} to {}", path.string(), toDest);

    const std::string notebookName = path.stem().string();
    const auto reportPath = _reportsDir/(notebookName+"."+toDest);
    spdlog::info("Notebook report written in {}", reportPath.string());

    if (!jupyterCmd.run()) {
        return false;
    }

    return jupyterCmd.getReturnCode() == 0;
}
