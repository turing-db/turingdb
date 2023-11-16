#include "ToolInit.h"

#include "NotebookRunner.h"

#include "BioLog.h"
#include "PerfStat.h"

using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit("biorun");

    ArgParser& argParser = toolInit.getArgParser();
    argParser.setArgsDesc("notebook.ipynb ...");
    argParser.addOption("q", "Launch the notebooks in quiet mode.");
    argParser.addOption("convertonly", "Only convert the notebooks to a report, do not execute notebooks");
    argParser.addOption("html", "Export each notebook as an html report");
    argParser.addOption("pdf", "Export each notebook as a pdf report.");
    argParser.addOption("report", "Export each notebook as a custom pdf report.");

    toolInit.init(argc, argv);

    bool quiet = false;
    bool exportHTML = false;
    bool exportPDF = false;
    bool generateReport = false;
    bool execNotebooks = true;

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "q") {
            quiet = true;
        } else if (optName == "html") {
            exportHTML = true;
        } else if (optName == "pdf") {
            exportPDF = true;
        } else if (optName == "report") {
            generateReport = true;
        } else if (optName == "convertonly") {
            execNotebooks = false;
        } 
    }

    // Run notebooks
    NotebookRunner notebookRunner(toolInit.getOutputsDir(), toolInit.getReportsDir());
    notebookRunner.setQuiet(quiet);
    notebookRunner.setExecEnabled(execNotebooks);
    notebookRunner.setExportHTML(exportHTML);
    notebookRunner.setExportPDF(exportPDF);
    notebookRunner.setGenerateReport(generateReport);

    for (const auto& arg : argParser.args()) {
        notebookRunner.addNotebook(arg);
    }

    if (!notebookRunner.run()) {
        BioLog::printSummary();
        BioLog::destroy();
        PerfStat::destroy();
        return EXIT_FAILURE;
    }

    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();
    return EXIT_SUCCESS;
}
