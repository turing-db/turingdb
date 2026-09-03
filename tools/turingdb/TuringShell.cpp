#include "TuringShell.h"

#include <signal.h>
#include <regex>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <termios.h>
#include <iostream>

#include <argparse.hpp>
#include <linenoise.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <tabulate/table.hpp>
#include <termcolor/termcolor.hpp>
#include <range/v3/view/drop.hpp>

#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
#include "LineNoiseHandle.h"
#include "LocalMemory.h"

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"

#include "columns/Block.h"
#include "columns/Column.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "GraphPath.h"

#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListUtils.h"

#include "versioning/ChangeResult.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"

#include "FileUtils.h"

#include "Panic.h"
#include "Profiler.h"

using namespace db;

namespace rg = ranges;
namespace rv = ranges::views;

namespace {

constexpr size_t HISTORY_MAX_LEN = 1500;
const std::string DEFAULT_HISTORY_FILE = ".turing_history";
const char* whiteChars = " \n\r\t";

// Remove whitespace in front of a string
inline void trim(std::string& str) {
    str.erase(str.begin(), std::find_if(str.begin(), str.end(),
                                        [](char ch) {
                                            return !isspace(ch);
                                        }));
}

inline std::string_view getFirstWord(const std::string& str) {
    const auto pos = str.find_first_of(" \t\r\n");
    if (pos == std::string::npos) {
        return std::string_view(str.c_str(), str.size());
    }

    return std::string_view(str.c_str(), pos);
}

void extractWords(std::vector<std::string>& words, const std::string& line) {
    size_t pos = 0;
    while (pos < line.size()) {
        // Skip whitespace
        pos = line.find_first_not_of(whiteChars, pos);
        if (pos == std::string::npos) {
            // We went to the end of the line
            return;
        }

        // Get position of the end of the word
        size_t newPos = line.find_first_of(whiteChars, pos);
        if (newPos == std::string::npos) {
            newPos = line.size();
        }

        words.emplace_back(std::string(line.c_str() + pos, newPos - pos));
        pos = newPos;
    }
}

// Commands
void helpCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    shell.printHelp();
}

void quitCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    shell.stop();
}

void changeDBCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    std::string graphName;
    argparse::ArgumentParser argParser("cd",
                                       "",
                                       argparse::default_arguments::help,
                                       false);
    argParser.add_description("Print Turing Shell help");
    argParser.add_argument("graph")
        .nargs(1)
        .metavar("graph_name")
        .store_into(graphName);

    try {
        argParser.parse_args(args);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing arguments: {}", e.what());
        return;
    }

    if (graphName.empty()) {
        spdlog::error("Graph name can not be empty");
        return;
    }

    if (!shell.setGraphName(graphName)) {
        spdlog::error("Graph {} does not exist", graphName);
        return;
    }
}

void checkoutCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    std::string hashStr;

    argparse::ArgumentParser argParser("checkout", "", argparse::default_arguments::help, false);
    argParser.add_description("Checkout specific commit or change of current graph");
    argParser.add_argument("hash")
        .nargs(1)
        .metavar("hash")
        .default_value("")
        .help("Commit hash or change-{id}")
        .store_into(hashStr);

    try {
        argParser.parse_args(args);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing arguments: {}", e.what());
        return;
    }

    constexpr std::string_view changePrefix = "change-";

    if (hashStr.empty()) {
        shell.setChangeID(ChangeID::head());
        return;
    }

    if (hashStr.size() > changePrefix.size()) {
        if (hashStr.substr(0, changePrefix.size()) == changePrefix) {
            // Parsing a change
            hashStr = hashStr.substr(changePrefix.size());
            const auto changeRes = ChangeID::fromString(hashStr);

            if (!changeRes) {
                spdlog::error("{} is not a valid change id", hashStr);
                return;
            }

            const auto currentChange = shell.getChangeID();

            if (currentChange != changeRes.value()) {
                shell.setChangeID(changeRes.value());
            }

            return;
        }
    }

    const auto hashRes = CommitHash::fromString(hashStr);

    if (!hashRes) {
        spdlog::error("{} is not a valid commit hash", hashStr);
        return;
    }

    const auto currentCommit = shell.getCommitHash();

    if (currentCommit != hashRes.value()) {
        shell.setCommitHash(hashRes.value());
    }
}

void quietCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    if (args.size() != 1) {
        spdlog::error("The quiet command does not accept any argument");
        return;
    }

    shell.setQuiet(true);
}

void unquietCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    if (args.size() != 1) {
        spdlog::error("The unquiet command does not accept any argument");
        return;
    }

    shell.setQuiet(false);
}

void readCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    std::string fileName;

    argparse::ArgumentParser argParser("read", "", argparse::default_arguments::help, false);
    argParser.add_description("Execute a script from the local file system");
    argParser.add_argument("file")
        .nargs(1)
        .metavar("file")
        .store_into(fileName);

    try {
        argParser.parse_args(args);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing arguments: {}", e.what());
        return;
    }

    std::string expandedPath;
    FileUtils::expandPath(fileName, expandedPath);
    if (!FileUtils::readContent(expandedPath, line)) {
        spdlog::error("Can not read content of file {}", expandedPath);
        line.clear();
        return;
    }
}

void connectCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    std::string address;
    std::string port;

    argparse::ArgumentParser argParser("connect", "", argparse::default_arguments::help, false);
    argParser.add_description("Connect to a remote turingdb server");
    argParser.add_argument("address")
        .nargs(1)
        .metavar("addr")
        .default_value("127.0.0.1")
        .help("The address of the server you want to connect to")
        .store_into(address);

    argParser.add_argument("port")
        .nargs(1)
        .metavar("port")
        .default_value("6666")
        .help("The port of the server you want to connect to")
        .store_into(port);

    try {
        argParser.parse_args(args);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing arguments: {}", e.what());
        return;
    }

    try {
        shell.connectRemote(address, port);
    } catch (const TuringException& e) {
        spdlog::error("Could not connect to client: {}", e.what());
    }
}

void disconnectCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    if (args.size() != 1) {
        spdlog::error("The disconnect command does not accept any argument");
        return;
    }

    shell.disconnectRemote();
}

void shCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
    // Get the user's shell from $SHELL, fallback to /bin/bash
    const char* shellPath = getenv("SHELL");
    if (shellPath == nullptr) {
        shellPath = "/bin/bash";
    }

    const pid_t pid = fork();
    if (pid == -1) {
        spdlog::error("Failed to fork process");
        return;
    }

    if (pid == 0) {
        // Child process
        if (args.size() < 2) {
            // No arguments: open interactive shell
            execl(shellPath, shellPath, nullptr);
        } else {
            // Build the command from all arguments after "sh"
            std::string cmd;
            for (size_t i = 1; i < args.size(); ++i) {
                if (i > 1) {
                    cmd += ' ';
                }
                cmd += args[i];
            }
            // Execute the command in the user's shell
            execl(shellPath, shellPath, "-c", cmd.c_str(), nullptr);
        }
        // If execl returns, it failed
        exit(EXIT_FAILURE);
    }

    // Parent process: wait for child to complete
    int status = 0;
    waitpid(pid, &status, 0);

    if (WIFEXITED(status)) {
        const int exitCode = WEXITSTATUS(status);
        if (exitCode != 0) {
            spdlog::warn("Command exited with status {}", exitCode);
        }
    } else if (WIFSIGNALED(status)) {
        spdlog::warn("Command terminated by signal {}", WTERMSIG(status));
    }
}

}

TuringShell::TuringShell(TuringDB& turingDB,
                         LocalMemory* mem,
                         LineNoiseHandle* lineNoiseHandle)
    : _turingDB(turingDB),
    _client("127.0.0.1", "6666", mem),
    _mem(mem),
    _lineNoiseHandle(lineNoiseHandle)
{
    _localCommands.emplace("q", Command {quitCommand});
    _localCommands.emplace("quit", Command {quitCommand});
    _localCommands.emplace("exit", Command {quitCommand});
    _localCommands.emplace("help", Command {helpCommand});
    _localCommands.emplace("cd", Command {changeDBCommand});
    _localCommands.emplace("checkout", Command {checkoutCommand});
    _localCommands.emplace("quiet", Command {quietCommand});
    _localCommands.emplace("unquiet", Command {unquietCommand});
    _localCommands.emplace("read", Command {readCommand});
    _localCommands.emplace("sh", Command {shCommand});
    _localCommands.emplace("shell", Command {shCommand});
    _localCommands.emplace("connect", Command {connectCommand});
    _localCommands.emplace("disconnect", Command {disconnectCommand});
}

TuringShell::~TuringShell() {
}

void TuringShell::startLoop() {
    _threadID = ::pthread_self();

    // Setup SIGUSR1 signal
    // Doesn't do anything by default, it only
    // interrupts ::read() (default unix behaviour)
    struct sigaction sa {};
    sa.sa_handler = [](int) {};
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGUSR1, &sa, nullptr);

    // Prepare prompt
    char* line = nullptr;
    std::string shellPrompt = composePrompt();

    // History settings
    linenoiseHistorySetMaxLen(HISTORY_MAX_LEN);

    // Get history file path
    const char* homeEnvVar = getenv("HOME");
    if (!homeEnvVar) {
        panic("$HOME environment variable not found");
    }

    const std::string historyFilePath = std::string(homeEnvVar) + "/" + DEFAULT_HISTORY_FILE;

    const int restoreRes = linenoiseHistoryLoad(historyFilePath.c_str());
    if (restoreRes < 0) {
        spdlog::error("Can not restore history from file {}", historyFilePath);
    }

    while (_running.load()) {
        errno = 0;

        // Async Lineoise API
        linenoiseEditStart(_lineNoiseHandle->getState(), -1, -1,
                           _lineNoiseHandle->getBuffer(),
                           LineNoiseHandle::BUFSIZE,
                           shellPrompt.c_str());
        _lineNoiseHandle->setActive();

        line = linenoiseEditMore;
        while ((line = linenoiseEditFeed(_lineNoiseHandle->getState())) == linenoiseEditMore) { }

        _lineNoiseHandle->setInactive();
        linenoiseEditStop(_lineNoiseHandle->getState());

        if (line == nullptr) {
            if (errno == EAGAIN) {
                // Ctrl+C -> Just redisplay the prompt
                fmt::println("If you meant to exit the shell, use 'Ctrl+D' instead.");
                shellPrompt = composePrompt();
                continue;
            }

            // Ctrl+D (EOF) or error — actually quit
            break;
        }

        std::string lineStr(line);
        if (lineStr.empty()) {
            linenoiseFree(line);
            continue;
        }

        processLine(lineStr);
        linenoiseHistoryAdd(line);

        shellPrompt = composePrompt();
        linenoiseFree(line);
    }

    if (linenoiseHistorySave(historyFilePath.c_str()) < 0) {
        spdlog::error("Failed to save history file {}", historyFilePath);
    }
}

std::string TuringShell::composePrompt() {
    if (_remoteConnected) {
        const auto remoteCommit = _client.getCommitHash();
        const auto remoteGraph = _client.getGraphName();
        return remoteCommit == CommitHash::head()
                 ? fmt::format("{}:{}> ", _remoteAddress, remoteGraph)
                 : fmt::format("{}:{}(detached {:x})> ", _remoteAddress, remoteGraph, remoteCommit.get());
    }

    const std::string basePrompt = "turing";
    if (_changeID == ChangeID::head()) {
        return _hash == CommitHash::head()
                 ? fmt::format("{}:{}> ", basePrompt, _graphName)
                 : fmt::format("{}:{}(detached {:x})> ", basePrompt, _graphName, _hash.get());
    }

    return _hash == CommitHash::head()
             ? fmt::format("{}:{}@{:x}> ", basePrompt, _graphName, _changeID.get())
             : fmt::format("{}:{}@{:x}(detached {:x})> ", basePrompt, _graphName, _changeID.get(), _hash.get());
}

void asString(std::string& out, const db::Path& path) {
    if (path.empty()) {
        return;
    }

    const auto reversed = path | std::views::reverse;
    size_t i = 0;
    for (auto val : reversed) {
        if (i % 2 == 0) {
            // NodeID
            out += fmt::format("({})", val);
        } else {
            // EdgeID
            out += fmt::format("-[{}]->", val);
        }
        ++i;
    }
}

void asString(std::string& out, const db::EntityList& list) {
    if (list.empty()) {
        return;
    }

    const auto& entries = list.getEntries();

    size_t i = 0;
    for (const auto& [type, val] : entries) {
        if (i++ != 0) {
            out += ", ";
        }

        if (type == EntityType::Node) {
            out += fmt::format("({})", val);
        } else {
            out += fmt::format("[{}]", val);
        }
    }
}

void asString(std::string& out, db::ValueType v) {
    out += ValueTypeName::value(v);
}

template <int I>
void asString(std::string& out, const TemplateCommitHash<I>& hash) {
    out += fmt::format("{:x}", hash.get());
}

void asString(std::string& out, const PropertyNull) {
    out += "null";
}

void asString(std::string& out, std::span<const float> embedding) {
    out += "[";

    if (embedding.size() > 0) {
        out += fmt::format("{}", embedding[0]);

        for (size_t i = 1; i < embedding.size(); ++i) {
            out += ", ";
            out += fmt::format("{}", embedding[i]);
        }
    }

    out += "]";
}

template <typename T>
void asString(std::string& out, const T& value) {
    // @_ref HistoryStep uses double escaped new line (\\n) so that it is valid JSON
    // if the `/query -d "history"` endpoint is hit. When writing to CLI we replace double
    // escaped with single escape so that it is rendered in terminal correctly.
    if constexpr (std::same_as<T, std::string>) {
        const std::regex re(R"(\\n)");
        out += std::regex_replace(value, re, "\n");
        return;
    }

    if constexpr (std::same_as<T, CustomBool>) {
        out += fmt::format("{}", value._boolean);
    } else {
        out += fmt::format("{}", value);
    }
}

template <typename T>
void asString(std::string& out, const std::optional<T>& value) {
    if (value) {
        asString(out, *value);
    } else {
        out += "null";
    }
}

// Forward declare so ListElementView overload sees this: an element may be a ListView itself
void asString(std::string& out, ListView lv);

void asString(std::string& out, const ListElementView v) {
    const auto writeTyped = [&out]<typename T>(const ListElementView ele) {
        const T typed = ele.getAs<T>();
        asString(out, typed);
    };

    const ListBufferTypeTag tag = v.getTag();
    ListTagDispatcher writer {._tag = tag};
    writer.execute(writeTyped, v);
}

void asString(std::string& out, const ListView lv) {
    if (lv.empty()) {
        out += "[]";
        return;
    }

    out += '[';

    const ListElementView fst = lv.front();
    asString(out, fst);

    for (const ListElementView ele : lv.elements() | rv::drop(1)) {
        out += ", ";
        asString(out, ele);
    }

    out += ']';
}

template <typename T>
void tabulateWrite(tabulate::RowStream& rs, const T& value) {
    std::string out;
    asString(out, value);
    rs << out;
}

struct TabulateCell {
    tabulate::RowStream& _rowStream;
    size_t _row {0};

    template <typename T>
    void operator()(const ColumnVector<T>* column) {
        tabulateWrite(_rowStream, column->at(_row));
    }

    template <typename T>
    void operator()(const ColumnConst<T>* column) {
        tabulateWrite(_rowStream, column->at(_row));
    }
};

void tabulateCell(tabulate::RowStream& rs, const Column* column, size_t row) {
    TabulateCell cell(rs, row);

    using Dispatcher = ColumnSingleDispatcher<OutputtedTypes::Allowed,
                                              TabulateCell,
                                              OutputtedTypes::Excluded>;

    Dispatcher::dispatch(column, cell);
}

void queryCallback(size_t execCount, const Dataframe* df, tabulate::Table& table) {
    const size_t rowCount = df->getLogicalRowCount();

    if (execCount == 0) {
        // Write header row
        tabulate::RowStream headerRow;
        for (const NamedColumn* namedCol : df->cols()) {
            const std::string_view name = namedCol->getName();
            if (name.empty()) {
                const ColumnTag tag = namedCol->getTag();
                headerRow << "$" + std::to_string(tag.getValue());
            } else {
                headerRow << name;
            }
        }

        table.add_row(std::move(headerRow));
    }

    // Write data rows
    for (size_t i = 0; i < rowCount; ++i) {
        tabulate::RowStream rs;
        for (const NamedColumn* namedCol : df->cols()) {
            tabulateCell(rs, namedCol->getColumn(), i);
        }

        table.add_row(std::move(rs));
    }
}

namespace {

class TuringShellNLSink : public NLOutputSink {
public:
    explicit TuringShellNLSink(bool quiet)
        : _quiet(quiet)
    {
    }

    void setColumnNames(std::span<const std::string_view> names) override {
        _columnNames.assign(names.begin(), names.end());

        if (_quiet) {
            return;
        }

        tabulate::RowStream headerRow;
        std::string header;
        for (size_t columnIndex = 0; columnIndex < _columnNames.size(); columnIndex++) {
            columnHeader(header, columnIndex);
            headerRow << header;
        }

        _table.add_row(std::move(headerRow));
        _headerWritten = true;
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        if (_quiet) {
            _rowCount += rowCount;
            return;
        }

        if (!_headerWritten) {
            tabulate::RowStream headerRow;
            std::string header;
            for (size_t columnIndex = 0; columnIndex < chunks.size(); columnIndex++) {
                columnHeader(header, columnIndex);
                headerRow << header;
            }

            _table.add_row(std::move(headerRow));
            _headerWritten = true;
        }

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            tabulate::RowStream rs;
            for (const Column* col : chunks) {
                tabulateCell(rs, col, rowIndex);
            }

            _table.add_row(std::move(rs));
            _rowCount++;
        }
    }

    tabulate::Table& getTable() { return _table; }
    size_t getRowCount() const { return _rowCount; }

private:
    tabulate::Table _table;
    std::vector<std::string> _columnNames;
    size_t _rowCount {0};
    bool _headerWritten {false};
    bool _quiet {false};

    void columnHeader(std::string& header, size_t columnIndex) const {
        const bool isNamed = columnIndex < _columnNames.size() && !_columnNames[columnIndex].empty();
        if (isNamed) {
            header = _columnNames[columnIndex];
        } else {
            header = "$" + std::to_string(columnIndex);
        }
    }
};

}

void TuringShell::runMLIRQuery(std::string_view query) {
    if (_remoteConnected) {
        spdlog::error("#v3 is only available in local mode");
        return;
    }

    TuringShellNLSink sink(_quiet);
    QueryStatus status;
    QueryInterpreterV3 interp(&_turingDB.getSystemManager());
    interp.execute(status, query, _graphName, _hash, _changeID, _mem, &sink);

    if (_mem) {
        _mem->clear();
    }

    if (!status.isOk()) {
        if (status.hasErrorMessage()) {
            std::string errorMsg = status.getError();
            formatMessage(errorMsg);
            spdlog::error("{}: {}", QueryStatusDescription::value(status.getStatus()), errorMsg);
        } else {
            spdlog::error("{}", QueryStatusDescription::value(status.getStatus()));
        }
        return;
    }

    if (!_quiet) {
        std::cout << sink.getTable() << "\n";
    }

    std::cout << "Query returned " << sink.getRowCount() << " rows.\n";
    std::cout << "Query executed in " << status.getTotalTime().count() << " ms.\n";
}

// Cleans double-escaped characters to single-escaped characters
void TuringShell::formatMessage(std::string& msg) {
    const std::regex newLine(R"(\\n)");
    const std::regex tab(R"(\\t)");
    const std::regex quotes(R"(\\")");
    const std::regex backSlash(R"(\\\\)");
    const std::regex forwardSlash(R"(\\/)");
    msg = std::regex_replace(msg, newLine, "\n");
    msg = std::regex_replace(msg, tab, "\t");
    msg = std::regex_replace(msg, quotes, "\"");
    msg = std::regex_replace(msg, backSlash, "\\");
    msg = std::regex_replace(msg, forwardSlash, "/");
}

void TuringShell::processLine(std::string& line) {
    {
        std::string profilerOutput;
        Profiler::dumpAndClear(profilerOutput);
        if (!profilerOutput.empty()) {
            fmt::print("{}\n", profilerOutput);
        }
    }

    // Remove leading whitespace
    trim(line);

    // Check if it is a local command
    const auto cmdName = getFirstWord(line);
    const auto localCmdIt = _localCommands.find(cmdName);
    if (localCmdIt != _localCommands.end()) {
        Command::Words words;
        extractWords(words, line);

        line.clear();
        localCmdIt->second._func(words, *this, line);

        if (line.empty()) {
            return;
        }
    }

    // Check for #v3 prefix to route through the MLIR executor
    constexpr std::string_view v3Prefix = "#v3 ";
    const bool useMLIR = line.size() >= v3Prefix.size() && line.substr(0, v3Prefix.size()) == v3Prefix;
    if (useMLIR) {
        line = line.substr(v3Prefix.size());
        trim(line);
        runMLIRQuery(line);
        return;
    }

    // Execute query
    tabulate::Table table;
    size_t rowCount = 0;

    QueryStatus res;
    Milliseconds remoteQueryTime {0};
    {
        size_t execCount = 0;

        auto shellOutPutCallBack = [&table, &execCount, &rowCount, this](const Dataframe* df) -> void {
            rowCount += df->getLogicalRowCount();

            if (_quiet) {
                return;
            }

            queryCallback(execCount++, df, table);
        };

        QueryCallbacks callbacks;
        callbacks.setOnOutputData(shellOutPutCallBack);

        if (_remoteConnected) {
            try {
                const TimePoint start = Clock::now();
                res = _client.sendQuery(line, shellOutPutCallBack);
                const TimePoint end = Clock::now();

                remoteQueryTime = end - start;

            } catch (const TuringException& e) {
                spdlog::error("Remote query failed: {}", e.what());
                disconnectRemote();
            }

        } else {
            const QueryState state(_graphName, _mem, &_turingDB.getDefaultQueryConfig(), &callbacks, _hash, _changeID);
            res = _turingDB.query(line, state);
        }
    }

    checkShellContext();

    if (_mem) {
        _mem->clear();
    }

    if (!res.isOk()) {
        if (res.hasErrorMessage()) {
            std::string errorMsg = res.getError();
            formatMessage(errorMsg);
            spdlog::error("{}: {}", QueryStatusDescription::value(res.getStatus()),
                          errorMsg);
        } else {
            spdlog::error("{}", QueryStatusDescription::value(res.getStatus()));
        }
        return;
    }

    if (!_quiet) {
        std::cout << table << "\n";
    }

    {
        std::string profilerOutput;
        Profiler::dumpAndClear(profilerOutput);
        if (!profilerOutput.empty()) {
            fmt::print("{}\n", profilerOutput);
        }
    }

    std::cout << "Query returned " << rowCount << " rows.\n";
    std::cout << "Query executed in " << res.getTotalTime().count() << " ms.\n";

    if (_remoteConnected) {
        std::cout << "Remote query executed in " << remoteQueryTime.count() << " ms.\n";
    }
}

bool TuringShell::setGraphName(const std::string& graphName) {
    if (_remoteConnected) {
        _client.setGraphName(graphName);
        _client.setCommitHash(CommitHash::head());
        return true;
    }

    SystemAccessor system = _turingDB.getSystemManager().accessShared();
    if (system.getGraph(graphName) == nullptr) {
        return false;
    }

    _hash = CommitHash::head();
    _graphName = graphName;
    return true;
}

bool TuringShell::setChangeID(ChangeID changeID) {
    _hash = CommitHash::head();

    if (_remoteConnected) {
        _client.setChangeID(changeID);
        _client.setCommitHash(CommitHash::head());
        return true;
    }

    SystemAccessor system = _turingDB.getSystemManager().accessShared();
    auto tx = system.openTransaction(_graphName, _hash, changeID);
    if (!tx) {
        spdlog::error("Can not checkout change: {}", tx.error().fmtMessage());
        return false;
    }

    if (!tx->isValid()) {
        spdlog::error("Can not checkout change");
        return false;
    }

    _changeID = changeID;
    return true;
}

bool TuringShell::setCommitHash(CommitHash hash) {
    if (_remoteConnected) {
        _client.setCommitHash(hash);
        return true;
    }

    SystemAccessor system = _turingDB.getSystemManager().accessUnique();
    auto tx = system.openTransaction(_graphName, hash, _changeID);

    if (!tx) {
        if (tx.error().getType() == ChangeErrorType::COMMIT_NOT_LOADED) {
            // Commit exists but isn't hydrated — load it now
            spdlog::info("Loading commit {:x}...", hash.get());
            auto loadRes = system.loadCommit(_graphName, hash);
            if (!loadRes) {
                spdlog::error("Failed to load commit: {}", loadRes.error().fmtMessage());
                return false;
            }
            // Re-open transaction after loading
            tx = system.openTransaction(_graphName, hash, _changeID);
        }

        if (!tx) {
            spdlog::error("Can not switch commit: {}", tx.error().fmtMessage());
            return false;
        }
    }

    if (!tx->isValid()) {
        spdlog::error("Can not switch commit");
        return false;
    }

    _hash = hash;
    return true;
}

void TuringShell::printHelp() const {
    for (const auto& entry : _localCommands) {
        std::cout << entry.first << "\n";
    }

    std::cout << "\n";
}

void TuringShell::stop() {
    _running.store(false);
    ::pthread_kill(_threadID, SIGUSR1);
}

void TuringShell::connectRemote(const std::string& address, const std::string& port) {
    _client.disconnect();
    _client.setRemoteAddress(address);
    _client.setRemotePort(port);
    _client.setGraphName(_graphName);
    _client.setCommitHash(_hash);
    _client.setChangeID(_changeID);
    _client.connect();

    _remoteAddress = address;
    _remotePort = port;
    _remoteConnected = true;
}

void TuringShell::disconnectRemote() {
    _client.disconnect();
    _remoteAddress.clear();
    _remotePort.clear();
    _remoteConnected = false;
}

void TuringShell::checkShellContext() {
    const Graph* graph = nullptr;
    {
        SystemAccessor system = _turingDB.getSystemManager().accessShared();
        graph = system.getGraph(_graphName);
    }

    if (graph == nullptr) {
        fmt::print("Graph '{}' does not exist anymore, switching back to default graph\n", _graphName);
        setGraphName("default");
        return;
    }

    if (_changeID == ChangeID::head()) {
        const FrozenCommitTx transaction = graph->openTransaction(_hash);
        if (transaction.isValid()) {
            return;
        }
    }

    Change* change = nullptr;
    {
        SystemAccessor system = _turingDB.getSystemManager().accessShared();
        const auto res = system.getChange(graph, _changeID);
        if (res) {
            change = res.value();
        }
    }

    if (change == nullptr) {
        fmt::print("Change '{:x}' does not exist anymore, switching back to head\n", _changeID.get());
        setChangeID(ChangeID::head());
        return;
    }

    if (auto tx = change->openWriteTransaction(); tx.isValid()) {
        return;
    }

    fmt::print("No commit matches hash {:x}, switching back to head\n", _hash.get());
    setCommitHash(CommitHash::head());
}
