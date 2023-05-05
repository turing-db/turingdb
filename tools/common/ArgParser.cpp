#include "ArgParser.h"

#include <cassert>
#include <iostream>
#include <stdlib.h>
#include <string>

#define INDENT "    "

ArgParser::ArgParser(const std::string& toolName)
    : _toolName(toolName)
{
}

ArgParser::~ArgParser() {
}

void ArgParser::setArgsDesc(const std::string& desc) {
    _argsDesc = desc;
}

void ArgParser::addOption(const std::string& optionName,
                          const std::string& desc,
                          bool expectsArg) {
    _optionMap[optionName] = Option(desc, expectsArg);
}

void ArgParser::parse(int argc, const char** argv) {
    std::string argStr;
    std::string optionName;
    bool inOption = false;

    for (int i = 1; i < argc; i++) {
        argStr = argv[i];
        if (argStr.empty()) {
            return;
        }

        if (argStr[0] == '-' && argStr.size() >= 2) {
            if (inOption) {
                handleOptionArgExpected(optionName);
                inOption = false;
            }

            optionName.assign(argStr, 1, std::string::npos);

            if (optionName == "h" || optionName == "help") {
                printHelp();
            } else {
                const auto foundIt = _optionMap.find(optionName);
                if (foundIt == _optionMap.end()) {
                    handleUnknownOption(optionName);
                }

                const bool expectsArg = foundIt->second._expectArg;
                if (expectsArg) {
                    inOption = true;
                } else {
                    _options.emplace_back(optionName, "");
                }
            }
        } else {
            if (inOption) {
                _options.emplace_back(optionName, argStr);
                inOption = false;
            } else {
                _args.emplace_back(argStr);
            }
        }
    }
}

void ArgParser::printHelp() const {
    std::cout << "Usage:\n";
    std::cout << INDENT << _toolName << " [options]";

    if (!_argsDesc.empty()) {
        std::cout << " " << _argsDesc;
    }

    std::cout << "\n\n";
    std::cout << "Options:\n";
    std::cout << INDENT << "-h, -help" << "\t" << "Display this help\n";

    for (const auto& option : _optionMap) {
        std::cout << INDENT << "-" << option.first
                  << INDENT << "\t" << option.second._desc << "\n";
    }

    std::cout << "\n";
    exit(EXIT_SUCCESS);
}

void ArgParser::handleUnknownOption(const std::string& name) const {
    std::cerr << "ERROR: unknown command line option " << name << "\n";
    printHelp();
}

void ArgParser::handleOptionArgExpected(const std::string& optionName) const {
    std::cerr << "ERROR: an argument was expected to be given to option "
              << optionName << "\n";
    printHelp();
}
