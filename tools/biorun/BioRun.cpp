#include "ToolInit.h"

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "NotebookRunner.h"

#include "FileUtils.h"
#include "LogUtils.h"

int main(int argc, const char** argv) {
    ToolInit toolInit("biorun");

    std::vector<std::string> notebooks;
    bool quietMode = false;
    bool convertOnly = false;
    bool exportHTML = false;
    bool exportPDF = false;
    bool generateReport = false;
    std::vector<NotebookRunner::EnvVar> nbArgs;

    auto& argParser = toolInit.getArgParser();

    argParser.add_argument("notebooks")
             .append()
             .nargs(argparse::nargs_pattern::any)
             .store_into(notebooks);

    argParser.add_argument("-q")
             .help("Launch the notebooks in quiet mode")
             .nargs(0)
             .store_into(quietMode);

    argParser.add_argument("-convertonly")
             .help("Only convert the notebooks to a report, do not execute notebooks")
             .nargs(0)
             .store_into(convertOnly);

    argParser.add_argument("-html")
             .help("Export each notebook as an html report")
             .nargs(0)
             .store_into(exportHTML);

    argParser.add_argument("-pdf")
             .help("Export each notebook as a pdf report")
             .nargs(0)
             .store_into(exportPDF);

    argParser.add_argument("-report")
             .help("Export each notebook as a custom pdf report.")
             .nargs(1)
             .store_into(generateReport);

    argParser.add_argument("-nbarg")
             .help("Set notebook argument arg_name=value")
             .nargs(argparse::nargs_pattern::at_least_one)
             .append()
             .action([&](const auto& value){
                const size_t it = value.find('=');
                if (it == std::string::npos) {
                    spdlog::error("Failed to parse notebook argument '{}' which must be of the form arg_name=value",
                                  value);
                    exit(EXIT_FAILURE);
                }

                const std::string argName = value.substr(0, it);
                const std::string argValue = value.substr(it + 1, value.size() - 1);

                nbArgs.emplace_back(NotebookRunner::EnvVar {
                    .argName = argName,
                    .argValue = argValue,
                });
             });

    toolInit.init(argc, argv);

    // Run notebooks
    NotebookRunner notebookRunner(toolInit.getOutputsDir(), toolInit.getReportsDir());
    notebookRunner.setQuiet(quietMode);
    notebookRunner.setExecEnabled(!convertOnly);
    notebookRunner.setExportHTML(exportHTML);
    notebookRunner.setExportPDF(exportPDF);
    notebookRunner.setGenerateReport(generateReport);
    notebookRunner.setEnvVars(std::move(nbArgs));

    for (const auto& path : notebooks) {
        if (!FileUtils::exists(path)) {
            logt::FileNotFound(path);
            return EXIT_FAILURE;
        }
        notebookRunner.addNotebook(path);
    }

    if (!notebookRunner.run()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
