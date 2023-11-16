#include "ArgParser.h"

#include <cassert>
#include <iostream>
#include <sstream>
#include <cstdlib>

#define INDENT_SIZE 4
#define INDENT "    "

std::string indent(size_t spaceCount) {
    return std::string(spaceCount, ' ');
}

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
                          const std::string& desc) {
    _optionMap[optionName] = Option {
        .desc = desc,
        .expectsArg = false,
    };
    _maxOptionSize = std::max(optionName.size(), _maxOptionSize);
}

void ArgParser::addOption(const std::string& optionName,
                          const std::string& desc,
                          const std::string& argName) {
    _optionMap[optionName] = Option {desc, argName, true};
    _maxOptionSize = std::max(optionName.size() + argName.size(), _maxOptionSize);
}

bool ArgParser::isOptionSet(const std::string& optionName) const {
    return std::find_if(_options.cbegin(),
                        _options.cend(),
                        [&](const auto& opt) {
                            return opt.first == optionName;
                        })
        != _options.end();
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

                const bool expectsArg = foundIt->second.expectsArg;
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
    // indentSize corresponds to format:
    // "    -OPTION [ARG] "
    size_t indentSize = 1 + _maxOptionSize + 6;
    std::stringstream content;
    content << "Usage:\n";
    content << INDENT << _toolName << " [options]";

    if (!_argsDesc.empty()) {
        content << " " << _argsDesc;
    }

    content << "\n\n";
    content << "Options:\n";
    content << INDENT
            << "-h, -help"
            << indent(indentSize - 10)
            << "Display this help\n";

    for (const auto& option : _optionMap) {
        size_t printedSize = 0;
        std::stringstream optionContent;
        optionContent << INDENT << "-" << option.first;
        printedSize += 1 + option.first.size() + 1;

        if (option.second.expectsArg) {
            if (option.second.argName.empty()) {
                optionContent << " [arg]";
                printedSize += 6;
            } else {
                optionContent << " [" << option.second.argName << "]";
                printedSize += 3 + option.second.argName.size();
            }
        }

        optionContent << indent(indentSize - printedSize) << option.second.desc << "\n";
        content << optionContent.str();
    }

    content << "\n";
    std::cout << content.str();
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
