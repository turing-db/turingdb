#include "ShellCompletion.h"

#include <ctype.h>
#include <poll.h>
#include <algorithm>

#include <linenoise.h>

using namespace db;

namespace {

// linenoise callbacks are plain C function pointers, with no user data to carry the instance.
ShellCompletion* activeCompletion = nullptr;

constexpr std::string_view openerChars = "([{";
constexpr std::string_view closerChars = ")]}";
constexpr std::string_view quoteChars = "\"'`";
constexpr std::string_view arrowChars = "<->";
constexpr std::string_view wordChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
constexpr std::string_view whiteChars = " \t";

// Characters an opener may be closed in front of, so that typing a bracket before
// existing text does not push a stray closer into the middle of it.
constexpr std::string_view autoCloseFollowers = " \t)]},;";

constexpr char backspaceKey = 127;
constexpr char controlHKey = 8;

struct Arrow {
    std::string_view _text;
    size_t _cursor {0};
};

constexpr Arrow arrowForms[] = {
    {"-->", 3},
    {"-[]->", 2},
    {"<--", 3},
    {"<-[]-", 3},
    {"--", 2},
    {"-[]-", 2},
};

constexpr std::string_view cypherKeywords[] = {
    "ADD",
    "ALL",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "ASCENDING",
    "AVAILABLE",
    "BY",
    "CALL",
    "CASE",
    "CHANGE",
    "COLLECT",
    "COMMIT",
    "CONNECT",
    "CONSTRAINT",
    "CONTAINS",
    "COSINE",
    "COUNT",
    "CREATE",
    "CSV",
    "DELETE",
    "DESC",
    "DESCENDING",
    "DETACH",
    "DIMENSION",
    "DISTINCT",
    "DO",
    "DROP",
    "ELSE",
    "EMBEDDING",
    "EMBEDDINGS",
    "END",
    "ENDS",
    "ERROR",
    "EUCLID",
    "EXISTS",
    "EXPLAIN",
    "EXTENSIONS",
    "EXTRACT",
    "FAIL",
    "FALSE",
    "FILTER",
    "FOR",
    "FROM",
    "GML",
    "GRAPH",
    "GRAPHS",
    "HEADERS",
    "IF",
    "IN",
    "INDEX",
    "INDEXES",
    "INSTALL",
    "IS",
    "JSONL",
    "LIMIT",
    "LIST",
    "LOAD",
    "MANDATORY",
    "MATCH",
    "MERGE",
    "MERGE_DATAPARTS",
    "METRIC",
    "NEW",
    "NONE",
    "NOT",
    "NULL",
    "OF",
    "ON",
    "OPTIONAL",
    "OR",
    "ORDER",
    "PARQUET",
    "PROCEDURES",
    "PULL",
    "PUSH",
    "REMOVE",
    "REQUIRE",
    "RETURN",
    "SCALAR",
    "SEARCH",
    "SET",
    "SHORTESTPATH",
    "SHOW",
    "SINGLE",
    "SKIP",
    "STARTS",
    "SUBMIT",
    "THEN",
    "TRUE",
    "UNION",
    "UNIQUE",
    "UNWIND",
    "VECTOR",
    "WHEN",
    "WHERE",
    "WITH",
    "XOR",
    "YIELD",
};

char closerFor(char opener) {
    switch (opener) {
        case '(':
            return ')';
        break;
        case '[':
            return ']';
        break;
        case '{':
            return '}';
        break;
        default:
            return opener;
        break;
    }
}

bool canAutoClose(std::string_view line, size_t cursor) {
    if (cursor >= line.size()) {
        return true;
    }

    return autoCloseFollowers.find(line[cursor]) != std::string_view::npos;
}

// Length of the closing run a '[' opened: the bracket alone, or the bracket
// together with the tail of the relationship arrow it was expanded into.
size_t bracketRunLength(std::string_view line, size_t cursor) {
    const std::string_view rest = line.substr(cursor);
    if (rest.starts_with("]->")) {
        return 3;
    } else if (rest.starts_with("]-")) {
        return 2;
    } else if (rest.starts_with("]")) {
        return 1;
    }

    return 0;
}

size_t closingRunLength(char opener, std::string_view line, size_t cursor) {
    if (cursor >= line.size()) {
        return 0;
    } else if (opener == '[') {
        return bracketRunLength(line, cursor);
    } else if (line[cursor] == closerFor(opener)) {
        return 1;
    }

    return 0;
}

// A '[' typed on the dash of a pattern opens a whole relationship arrow rather than
// a bare bracket pair, so that '-[' becomes '-[]->' and '<-[' becomes '<-[]-'.
std::string_view arrowPairFor(char opener, std::string_view line, size_t cursor) {
    const bool afterDash = cursor > 0 && line[cursor - 1] == '-';

    if (opener != '[' || !afterDash) {
        return std::string_view();
    }

    const bool afterTip = cursor > 1 && line[cursor - 2] == '<';
    return afterTip ? "[]-" : "[]->";
}

bool startsWithNoCase(std::string_view text, std::string_view prefix) {
    if (text.size() < prefix.size()) {
        return false;
    }

    for (size_t i = 0; i < prefix.size(); ++i) {
        const int textChar = ::toupper(static_cast<unsigned char>(text[i]));
        const int prefixChar = ::toupper(static_cast<unsigned char>(prefix[i]));

        if (textChar != prefixChar) {
            return false;
        }
    }

    return true;
}

void addCandidate(std::vector<ShellCompletion::Candidate>& candidates,
                  std::string_view base,
                  std::string_view completion) {
    ShellCompletion::Candidate& candidate = candidates.emplace_back();

    candidate._text.assign(base);
    candidate._text.append(completion);
    candidate._cursor = candidate._text.size();
}

// A key that already has more input queued behind it comes from a paste rather than
// from typing, and pasted text carries the brackets it needs: pairing them again would
// leave a stray closer behind every one of them.
bool isPastedKey(int inputFileDescriptor) {
    struct pollfd poller {};

    poller.fd = inputFileDescriptor;
    poller.events = POLLIN;

    return ::poll(&poller, 1, 0) > 0;
}

void onCompletion(const char* prefix, linenoiseCompletions* completions) {
    if (!activeCompletion) {
        return;
    }

    std::vector<ShellCompletion::Candidate> candidates;
    activeCompletion->complete(prefix, candidates);

    for (const ShellCompletion::Candidate& candidate : candidates) {
        linenoiseAddCompletionWithCursor(completions, candidate._text.c_str(), candidate._cursor);
    }
}

int onKey(linenoiseState* state, int key) {
    if (!activeCompletion || isPastedKey(state->ifd)) {
        return 0;
    }

    const std::string_view line(state->buf, state->len);
    ShellCompletion::Edit edit;

    if (!activeCompletion->editForKey(static_cast<char>(key), line, state->pos, edit)) {
        return 0;
    }

    for (size_t deleted = 0; deleted < edit._deleteBefore; ++deleted) {
        linenoiseEditBackspace(state);
    }

    for (size_t deleted = 0; deleted < edit._deleteAfter; ++deleted) {
        linenoiseEditDelete(state);
    }

    if (!edit._insert.empty()) {
        linenoiseEditInsert(state, edit._insert.data(), edit._insert.size());
    }

    for (size_t back = edit._cursor; back < edit._insert.size(); ++back) {
        linenoiseEditMoveLeft(state);
    }

    for (size_t forward = 0; forward < edit._moveRight; ++forward) {
        linenoiseEditMoveRight(state);
    }

    return 1;
}

}

ShellCompletion::ShellCompletion() {
}

ShellCompletion::~ShellCompletion() {
}

void ShellCompletion::addCommand(std::string_view command) {
    const auto position = std::lower_bound(_commands.begin(), _commands.end(), command);
    if (position != _commands.end() && *position == command) {
        return;
    }

    _commands.emplace(position, command);
}

void ShellCompletion::install() {
    activeCompletion = this;

    linenoiseSetCompletionCallback(onCompletion);
    linenoiseSetKeyCallback(onKey);
}

bool ShellCompletion::editForKey(char key, std::string_view line, size_t cursor, Edit& edit) const {
    const bool isBackspace = key == backspaceKey || key == controlHKey;
    const bool isQuote = quoteChars.find(key) != std::string_view::npos;
    const bool isOpener = openerChars.find(key) != std::string_view::npos;
    const bool isCloser = closerChars.find(key) != std::string_view::npos;

    if (isBackspace) {
        return deletePair(line, cursor, edit);
    } else if (isCloser) {
        return stepOverCloser(key, line, cursor, edit);
    } else if (isQuote) {
        return stepOverCloser(key, line, cursor, edit) || openPair(key, line, cursor, edit);
    } else if (isOpener) {
        return openPair(key, line, cursor, edit);
    }

    return false;
}

bool ShellCompletion::deletePair(std::string_view line, size_t cursor, Edit& edit) {
    if (cursor == 0) {
        return false;
    }

    // closerFor hands back any other character unchanged, so without this only an opener
    // or a quote is read as one half of a pair - a doubled letter is two characters that
    // happen to be equal
    const char opener = line[cursor - 1];
    const bool opensAPair = openerChars.find(opener) != std::string_view::npos
                            || quoteChars.find(opener) != std::string_view::npos;

    if (!opensAPair) {
        return false;
    }

    const size_t runLength = closingRunLength(opener, line, cursor);
    if (runLength == 0) {
        return false;
    }

    edit._deleteBefore = 1;
    edit._deleteAfter = runLength;

    return true;
}

bool ShellCompletion::stepOverCloser(char key, std::string_view line, size_t cursor, Edit& edit) {
    const bool onOwnCloser = cursor < line.size() && line[cursor] == key;
    if (!onOwnCloser) {
        return false;
    }

    edit._moveRight = key == ']' ? bracketRunLength(line, cursor) : 1;

    return true;
}

bool ShellCompletion::openPair(char key, std::string_view line, size_t cursor, Edit& edit) {
    if (!canAutoClose(line, cursor)) {
        return false;
    }

    const std::string_view arrowPair = arrowPairFor(key, line, cursor);
    if (!arrowPair.empty()) {
        edit._insert.assign(arrowPair);
    } else {
        edit._insert.assign(1, key);
        edit._insert.push_back(closerFor(key));
    }

    edit._cursor = 1;

    return true;
}

void ShellCompletion::complete(std::string_view prefix, std::vector<Candidate>& candidates) const {
    completeArrow(prefix, candidates);

    if (candidates.empty()) {
        completeWord(prefix, candidates);
    }
}

void ShellCompletion::completeArrow(std::string_view prefix, std::vector<Candidate>& candidates) const {
    const size_t arrowStart = prefix.find_last_not_of(arrowChars) + 1;
    if (arrowStart == prefix.size()) {
        return;
    }

    const std::string_view typedArrow = prefix.substr(arrowStart);
    const std::string_view base = prefix.substr(0, arrowStart);

    for (const Arrow& arrow : arrowForms) {
        if (!arrow._text.starts_with(typedArrow)) {
            continue;
        }

        Candidate& candidate = candidates.emplace_back();

        candidate._text.assign(base);
        candidate._text.append(arrow._text);
        candidate._cursor = base.size() + arrow._cursor;
    }
}

void ShellCompletion::completeWord(std::string_view prefix, std::vector<Candidate>& candidates) const {
    const size_t wordStart = prefix.find_last_not_of(wordChars) + 1;
    const std::string_view word = prefix.substr(wordStart);

    if (word.empty()) {
        return;
    }

    const std::string_view base = prefix.substr(0, wordStart);
    const bool isFirstWord = base.find_first_not_of(whiteChars) == std::string_view::npos;

    if (isFirstWord) {
        for (const std::string& command : _commands) {
            if (startsWithNoCase(command, word)) {
                addCandidate(candidates, base, command);
            }
        }
    }

    for (const std::string_view keyword : cypherKeywords) {
        if (startsWithNoCase(keyword, word)) {
            addCandidate(candidates, base, keyword);
        }
    }
}
