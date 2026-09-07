#pragma once

#include <stddef.h>
#include <string>
#include <string_view>
#include <vector>

namespace db {

// Bracket pairing and TAB completion for the shell prompt. Both are expressed over a
// plain line and cursor position, so the rules stay independent of the line editor.
class ShellCompletion {
public:
    // Replace the _deleteBefore characters before the cursor and the _deleteAfter ones
    // after it with _insert, then place the cursor _cursor characters into _insert.
    // _moveRight instead steps the cursor over characters that are already there.
    struct Edit {
        std::string _insert;
        size_t _cursor {0};
        size_t _deleteBefore {0};
        size_t _deleteAfter {0};
        size_t _moveRight {0};
    };

    struct Candidate {
        std::string _text;
        size_t _cursor {0};
    };

    ShellCompletion();
    ~ShellCompletion();

    void addCommand(std::string_view command);
    void install();

    bool editForKey(char key, std::string_view line, size_t cursor, Edit& edit) const;
    void complete(std::string_view prefix, std::vector<Candidate>& candidates) const;

private:
    std::vector<std::string> _commands;

    static bool deletePair(std::string_view line, size_t cursor, Edit& edit);
    static bool stepOverCloser(char key, std::string_view line, size_t cursor, Edit& edit);
    static bool openPair(char key, std::string_view line, size_t cursor, Edit& edit);

    void completeArrow(std::string_view prefix, std::vector<Candidate>& candidates) const;
    void completeWord(std::string_view prefix, std::vector<Candidate>& candidates) const;
};

}
