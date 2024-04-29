#ifndef _BIO_TOOL_INIT_
#define _BIO_TOOL_INIT_

#include <string>
#include <memory>

namespace argparse {
class ArgumentParser;
}

class ToolInit {
public:
    ToolInit(const std::string& toolName);
    ~ToolInit();

    void setOutputDir(const std::string& outDir);

    void disableOutputDir() { _outputDirEnabled = false; }
    void createOutputDir();

    void printHelp() const;

    void init(int argc, const char** argv);

    const std::string& getOutputsDir() const { return _outputsDir; }
    const std::string& getReportsDir() const { return _reportsDir; }

    argparse::ArgumentParser& getArgParser() { return *_argParser; }

private:
    const std::string _toolName;
    std::string _outputsDir;
    std::string _reportsDir;
    std::unique_ptr<argparse::ArgumentParser> _argParser;
    bool _outputDirEnabled {true};

    void setupArgParser();
    void parseArguments(int argc, const char** argv);
};

#endif
