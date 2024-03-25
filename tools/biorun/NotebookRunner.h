#ifndef _NOTEBOOK_RUNNER_
#define _NOTEBOOK_RUNNER_

#include <filesystem>
#include <vector>

class NotebookRunner {
public:
    using Path = std::filesystem::path;
    using Notebooks = std::vector<Path>;

    struct EnvVar {
        std::string argName;
        std::string argValue;
    };

    NotebookRunner(const Path& outDir, const Path& reportsDir);
    ~NotebookRunner();

    void setQuiet(bool quiet) { _silent = quiet; }
    void setExportHTML(bool enable) { _exportHTML = enable; }
    void setExportPDF(bool enable) { _exportPDF = enable; }
    void setGenerateReport(bool enable) { _generateReport = enable; }
    void setExecEnabled(bool enable) { _execNotebooks = enable; }
    void setEnvVars(std::vector<EnvVar>&& vars) { _envVars = std::move(vars); };

    void addNotebook(const Path& path);

    bool run();

private:
    Path _outDir;
    Path _reportsDir;
    Notebooks _notebooks;
    std::vector<EnvVar> _envVars;

    bool _silent {false};
    bool _execNotebooks {true};
    bool _exportHTML {false};
    bool _exportPDF {false};
    bool _generateReport {false};

    bool runNotebook(const Path& path);
    bool executeNotebook(const Path& path);
    bool exportNotebook(const Path& path, const std::string& toDest);
    bool generateReport(const Path& path);
};

#endif
