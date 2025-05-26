#include "TuringShell.h"

#include <linenoise.h>
#include <tabulate/table.hpp>
#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <termcolor/termcolor.hpp>

#include "ChangeManager.h"
#include "Graph.h"
#include "TuringDB.h"
#include "columns/Block.h"
#include "columns/Column.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "FileUtils.h"
#include "Panic.h"
#include "Profiler.h"

using namespace db;

namespace {

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
    exit(EXIT_SUCCESS);
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
    shell.setQuiet(true);
}

void unquietCommand(const TuringShell::Command::Words& args, TuringShell& shell, std::string& line) {
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

    fileName = FileUtils::expandPath(fileName);
    if (!FileUtils::readContent(fileName, line)) {
        spdlog::error("Can not read content of file {}", fileName);
        line.clear();
        return;
    }
}

} // namespace

TuringShell::TuringShell(TuringDB& turingDB, LocalMemory* mem)
    : _turingDB(turingDB),
    _mem(mem)
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
}

TuringShell::~TuringShell() {
}

void TuringShell::startLoop() {
    std::string shellPrompt = composePrompt();
    char* line = NULL;
    std::string lineStr;
    while ((line = linenoise(shellPrompt.c_str())) != NULL) {
        lineStr = line;
        if (lineStr.empty()) {
            continue;
        }

        processLine(lineStr);

        linenoiseHistoryAdd(line);
        shellPrompt = composePrompt();
        linenoiseFree(line);
    }
}

std::string TuringShell::composePrompt() {
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

template <typename T>
void tabulateWrite(tabulate::RowStream& rs, const T& value) {
    rs << value;
}

template <typename T>
void tabulateWrite(tabulate::RowStream& rs, const std::optional<T>& value) {
    if (value) {
        rs << *value;
    } else {
        rs << "null";
    }
}

void tabulateWrite(tabulate::RowStream& rs, const CommitBuilder* commit) {
    rs << fmt::format("{:x}", commit->hash().get());
}

void tabulateWrite(tabulate::RowStream& rs, const Change* change) {
    rs << fmt::format("{:x}", change->id().get());
}

#define TABULATE_COL_CASE(Type, i)                        \
    case Type::staticKind(): {                            \
        const Type& src = *static_cast<const Type*>(col); \
        tabulateWrite(rs, src[i]);                        \
    } break;

#define TABULATE_COL_CONST_CASE(Type)                     \
    case Type::staticKind(): {                            \
        const Type& src = *static_cast<const Type*>(col); \
        tabulateWrite(rs, src.getRaw());                  \
    } break;


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

    // Execute query
    tabulate::Table table;

    auto queryCallback = [&table](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        for (size_t i = 0; i < rowCount; ++i) {
            tabulate::RowStream rs;
            for (const Column* col : block.columns()) {
                switch (col->getKind()) {
                    TABULATE_COL_CASE(ColumnVector<EntityID>, i)
                    TABULATE_COL_CASE(ColumnVector<types::UInt64::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<types::Int64::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<types::Double::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<types::String::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<types::Bool::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::UInt64::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::Int64::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::Double::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::String::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::Bool::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<std::string>, i)
                    TABULATE_COL_CASE(ColumnVector<const CommitBuilder*>, i)
                    TABULATE_COL_CASE(ColumnVector<const Change*>, i)
                    TABULATE_COL_CONST_CASE(ColumnConst<EntityID>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::UInt64::Primitive>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::Int64::Primitive>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::Double::Primitive>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::String::Primitive>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::Bool::Primitive>)

                    default: {
                        panic("can not print columns of kind {}", col->getKind());
                    }
                }
            }

            table.add_row(std::move(rs));
        }
    };

    const auto res = _quiet
                       ? _turingDB.query(line, _graphName, _mem, [](const Block&) {}, _hash, _changeID)
                       : _turingDB.query(line, _graphName, _mem, queryCallback, _hash, _changeID);

    checkShellContext();

    if (!res.isOk()) {
        if (res.hasErrorMessage()) {
            spdlog::error("{}: {}", QueryStatusDescription::value(res.getStatus()), res.getError());
        } else {
            spdlog::error("{}", QueryStatusDescription::value(res.getStatus()));
        }
        return;
    }

    std::cout << "Query executed in " << res.getTotalTime().count() << " ms.\n";

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
}

bool TuringShell::setGraphName(const std::string& graphName) {
    if (_turingDB.getSystemManager().getGraph(graphName) == nullptr) {
        return false;
    }

    _hash = CommitHash::head();
    _graphName = graphName;
    return true;
}

bool TuringShell::setChangeID(ChangeID changeID) {
    _hash = CommitHash::head();

    auto tx = _turingDB.getSystemManager().openTransaction(_graphName, _hash, changeID);
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
    auto tx = _turingDB.getSystemManager().openTransaction(_graphName, hash, _changeID);
    if (!tx) {
        spdlog::error("Can not switch commit: {}", tx.error().fmtMessage());
        return false;
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

void TuringShell::checkShellContext() {
    const auto* graph = _turingDB.getSystemManager().getGraph(_graphName);
    if (graph == nullptr) {
        fmt::print("Graph '{}' does not exist anymore, switching back to default graph\n", _graphName);
        setGraphName("default");
        return;
    }

    if (_changeID == ChangeID::head()) {
        FrozenCommitTx transaction = graph->openTransaction(_hash);
        if (transaction.isValid()) {
            return;
        }
    }

    auto res = _turingDB.getSystemManager().getChangeManager().getChange(graph, _changeID);
    if (!res) {
        fmt::print("Change '{:x}' does not exist anymore, switching back to head\n", _changeID.get());
        setChangeID(ChangeID::head());
        return;
    }

    auto* change = res.value();
    if (auto tx = change->openWriteTransaction(); tx.isValid()) {
        return;
    }

    fmt::print("No commit matches hash {:x}, switching back to head\n", _hash.get());
    setCommitHash(CommitHash::head());
}

