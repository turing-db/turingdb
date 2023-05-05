#include "ToolInit.h"

#include "NotebookRunner.h"

#include "BioLog.h"

using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit("biorun");

    ArgParser& argParser = toolInit.getArgParser();
    argParser.setArgsDesc("notebook.ipynb ...");
    argParser.addOption("q", "Launch the notebooks in quiet mode.", false);
    argParser.addOption("convertonly", "Only convert the notebooks to a report, do not execute notebooks", false);
    argParser.addOption("html", "Export each notebook as an html report", false);
    argParser.addOption("pdf", "Export each notebook as a pdf report.", false);

    toolInit.init(argc, argv);

    bool quiet = false;
    bool exportHTML = false;
    bool exportPDF = false;
    bool execNotebooks = true;

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "q") {
            quiet = true;
        } else if (optName == "html") {
            exportHTML = true;
        } else if (optName == "pdf") {
            exportPDF = true;
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

    for (const auto& arg : argParser.args()) {
        notebookRunner.addNotebook(arg);
    }

    notebookRunner.run();

    BioLog::printSummary();
    BioLog::destroy();

    return EXIT_SUCCESS;
}
