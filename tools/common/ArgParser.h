#ifndef _ARG_PARSER_
#define _ARG_PARSER_

#include <map>
#include <string>
#include <utility>
#include <vector>

class ArgParser {
public:
    using Args = std::vector<std::string>;
    using Options = std::vector<std::pair<std::string, std::string>>;

    ArgParser(const std::string& toolName);
    ~ArgParser();

    void setArgsDesc(const std::string& desc);
    void addOption(const std::string& optionName,
                   const std::string& desc);
    void addOption(const std::string& optionName,
                   const std::string& desc,
                   const std::string& argName);
    void printHelp() const;
    bool isOptionSet(const std::string& optionName) const;

    void parse(int argc, const char** argv);

    const Args& args() const { return _args; }
    const Options& options() const { return _options; }

private:
    struct Option {
        std::string desc;
        std::string argName;
        bool expectsArg {false};
    };

    std::string _toolName;
    std::string _argsDesc;
    std::map<std::string, Option> _optionMap;
    size_t _maxOptionSize = 0;
    Options _options;
    Args _args;

    void handleUnknownOption(const std::string& name) const;
    void handleOptionArgExpected(const std::string& optionName) const;
};

#endif
