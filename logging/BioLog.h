#ifndef _BIO_LOG_LOG_
#define _BIO_LOG_LOG_

#include <string>
#include <fstream>

namespace Log {

class Message;

class BioLog {
public:
    static void init();
    static void openFile(const std::string& file);

    static BioLog* getInstance();

    static void log(const Message& msg);
    static void echo(const std::string& str, bool newline=true);
    static void printSummary();

    static void destroy();

    bool isOk() const;

private:
    BioLog();
    void open(const std::string& logFile);
    void emit(const Message& msg);
    void print(const std::string& str);
    void _printSummary();
    void close();
    void flush();

    static BioLog* _instance;

    std::ofstream _outStream;
    int _stdoutFd {-1};
    int _stderrFd {-1};

    size_t _errorCount {0};
    size_t _fatalCount {0};
    size_t _infoCount {0};
    size_t _warningCount {0};
};

}

#endif
