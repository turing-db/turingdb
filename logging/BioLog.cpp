#include "BioLog.h"

#include <iostream>
#include <cctype>
#include <unistd.h>
#include <sstream>

#include "Message.h"

#include "BioAssert.h"

using namespace Log;

BioLog* BioLog::_instance = nullptr;

BioLog::BioLog()
{
}

void BioLog::init(const std::string& logFile) {
    if (_instance) {
        return;
    }

    _instance = new BioLog();
    _instance->open(logFile);
}

void BioLog::open(const std::string& logFile) {
    _outStream.open(logFile);
    if (!_outStream.is_open()) {
        std::cerr << "Error: failed to open the log file '" << logFile << "' for write.\n";
        abort();
    }
}

void BioLog::close() {
    _outStream.close();
}

void BioLog::destroy() {
    if (_instance) {
        _instance->close();
        delete _instance;
    }
    _instance = nullptr;
}

void BioLog::log(const Message& msg) {
    bioassert(_instance);
    _instance->emit(msg);
}

void BioLog::echo(const std::string& str) {
    bioassert(_instance);
    _instance->print(str);
}

void BioLog::print(const std::string& str) {
    _outStream << str << '\n';
    std::cout << str << '\n';
}

void BioLog::emit(const Message& msg) {
    const std::string& format = msg.getFormat();
    bioassert(!format.empty());
    bioassert(_instance);

    const MessageArgStorage* args = msg.getArgStorage();

    std::string errorStr;
    errorStr = "["; 
    errorStr += std::to_string(msg.getID());
    errorStr += "] ";

    switch (msg.getSeverity()) {
        case Message::INFO:
            _infoCount++;
            errorStr += "Info: ";
            break;

        case Message::WARNING:
            _warningCount++;
            errorStr += "Warning: ";
            break;

        case Message::ERROR:
            _errorCount++;
            errorStr += "Error: ";
            break;

        case Message::FATAL:
            _fatalCount++;
            errorStr += "Fatal: ";
            break;
    }

    const size_t formatSize = format.size();
    for (size_t i = 0; i < formatSize; i++) {
        if (format[i] == '$' && i < formatSize-1 && isdigit(format[i+1])) {
            const unsigned argPos = format[i+1]-'0';
            bioassert(argPos < args->_size);

            errorStr += args->_args[argPos];
            i++;
        } else {
            errorStr.push_back(format[i]);
        }
    }

    _outStream << errorStr << '\n';
    _outStream.flush();

    std::cout << errorStr << '\n';

    if (_fatalCount >= 1) {
        printSummary();
        destroy();
        exit(EXIT_FAILURE);
    }
}

void BioLog::printSummary() {
    if (_instance) {
        _instance->_printSummary();
    }
}

void BioLog::_printSummary() {
    std::ostringstream out;
    out << "\nSummary:\n";
    out << "-------------------------------------------------------------------\n";
    out << "Terminated";
    
    if (_fatalCount == 0 && _errorCount == 0 && _warningCount == 0) {
        out << " successfully";
    }
    out << " with ";

    out << _fatalCount;
    out << " Fatal, ";
    out << _errorCount;
    out << ((_errorCount == 1) ? " Error, " : " Errors, ");
    out << _warningCount;
    out << ((_warningCount == 1) ? " Warning" : " Warnings");
    out << ".\n";
    out << "-------------------------------------------------------------------";
    print(out.str());
}
