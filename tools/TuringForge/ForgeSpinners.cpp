#include "ForgeSpinners.h"

#include <stddef.h>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "BannerDisplay.h"

using namespace forge;

namespace {

// RAII guard for the terminal cursor: hides it on construction and restores it on
// destruction, so the cursor is shown again on every exit path from the render
// loop — including an exception thrown mid-frame. `active` lets the caller bind
// the guard unconditionally and only act when a cursor was actually hidden.
class CursorHider {
public:
    explicit CursorHider(bool active)
        : _active(active) {
        if (_active) {
            fmt::print("\033[?25l");
            fflush(stdout);
        }
    }

    ~CursorHider() {
        if (_active) {
            fmt::print("\033[?25h");
            fflush(stdout);
        }
    }

    CursorHider(const CursorHider&) = delete;
    CursorHider& operator=(const CursorHider&) = delete;

private:
    bool _active {false};
};

// Display width (in terminal cells) of a UTF-8 string: count the bytes that are
// not continuation bytes. The logo glyphs are all single-cell.
size_t displayWidth(const char* text) {
    size_t width = 0;
    for (const char* cursor = text; *cursor != '\0'; ++cursor) {
        if ((static_cast<unsigned char>(*cursor) & 0xC0) != 0x80) {
            ++width;
        }
    }
    return width;
}

// Overwrite row[column..] with text, clipped to the row. row must be ASCII so a
// byte offset equals a column offset.
void overlay(std::string& row, size_t column, std::string_view text) {
    for (size_t offset = 0; offset < text.size() && column + offset < row.size(); ++offset) {
        row[column + offset] = text[offset];
    }
}

// Wrap each spark glyph in an ember colour, in place, so the sparks stand out
// from the (default-coloured) hammer. The glyph picks the heat: '*' is the
// hottest, '`' the coolest, so sparks cool as they travel and change glyph.
// Applied only to the animated header rows, where these glyphs are unambiguous,
// after positioning is done — embedding the escape codes earlier would corrupt
// the fixed-width column maths in overlay().
void colorizeSparks(std::string& row) {
    // Most header rows carry no spark glyph; skip the rebuild and the allocation
    // entirely for them.
    if (row.find_first_of("*.`") == std::string::npos) {
        return;
    }

    static const std::string reset = "\033[0m";

    std::string colored;
    colored.reserve(row.size() + 16);
    for (const char glyph : row) {
        const char* color = nullptr;
        if (glyph == '*') {
            color = "\033[38;5;226m"; // hot — bright yellow-white
        } else if (glyph == '.') {
            color = "\033[38;5;208m"; // warm orange ember
        } else if (glyph == '`') {
            color = "\033[38;5;160m"; // cooling red ember
        }

        if (color != nullptr) {
            colored += color;
            colored += glyph;
            colored += reset;
        } else {
            colored += glyph;
        }
    }

    row.swap(colored);
}

// Build one frame of the "special" animation into `canvas`: ASCII hammer and
// spark rows drawn above the verbatim logo, then a status line. `frame` selects
// the strike phase. The animated rows are pure ASCII (so column maths is
// trivial); the multibyte logo lines are emitted unchanged. `canvas` is reused
// across frames: the static logo block is laid down once on the first call, and
// later calls only redraw the animated header rows and the status line.
void buildLogoFrame(size_t frame,
                    double elapsedSeconds,
                    double totalSeconds,
                    std::vector<std::string>& canvas) {
    constexpr size_t headerRows = 6;
    constexpr size_t marginCols = 9; // blank space left of the logo for the hammer to swing in

    const std::array<const char*, BannerDisplay::LOGO_LINE_COUNT>& logoLines = BannerDisplay::getLogoLines();

    // The hammer-swing region and the logo are a fixed size, so derive the row
    // width and total height once and keep them for every frame.
    static const size_t width = displayWidth(logoLines[0]) + marginCols;
    constexpr size_t statusRows = 1;
    constexpr size_t totalRows = headerRows + BannerDisplay::LOGO_LINE_COUNT + statusRows;

    // First call lays down the static logo block; subsequent calls leave it in
    // place and only the header rows and status line below are rewritten.
    if (canvas.size() != totalRows) {
        canvas.assign(totalRows, std::string());

        const std::string margin(marginCols, ' ');
        for (size_t row = 0; row < logoLines.size(); ++row) {
            canvas[headerRows + row] = margin + logoLines[row];
        }
    }

    // Clear the animated header rows back to blank ASCII before redrawing them.
    for (size_t row = 0; row < headerRows; ++row) {
        canvas[row].assign(width, ' ');
    }

    // The hammer pivots to the left of the logo and swings clockwise into its
    // top-left corner: handle rotates "\" -> "|" -> "/" -> "-", striking on "-".
    // Rather than ping-ponging the identical frames back, the recoil ("/", "|")
    // carries the struck embers drifting further up and out each frame, so the
    // upswing looks different from the downswing. Each pose is hand-drawn since
    // ASCII can't rotate a sprite.
    struct Step {
        size_t pose;       // 0 "\", 1 "|", 2 "/", 3 "-" (strike)
        size_t emberStage; // 0 none, 1 burst, 2 drift, 3 drift further
    };
    static const Step SEQUENCE[] = {
        {0, 0}, // downswing: wound up, no embers yet
        {1, 0},
        {2, 0},
        {3, 1}, // strike: embers burst at the corner
        {2, 2}, // recoil: embers drift up and out
        {1, 3}, // recoil: embers drift further still
    };
    const Step step = SEQUENCE[frame % (sizeof(SEQUENCE) / sizeof(SEQUENCE[0]))];

    switch (step.pose) {
        case 0: // "\" — wound up, head to the upper-left
            overlay(canvas[0], 0, "[##]");
            overlay(canvas[1], 3, "\\");
            overlay(canvas[2], 4, "\\");
            overlay(canvas[3], 5, "\\");
            break;
        case 1: // "|" — head straight up
            overlay(canvas[0], 2, "[##]");
            overlay(canvas[1], 3, "|");
            overlay(canvas[2], 3, "|");
            overlay(canvas[3], 3, "|");
            break;
        case 2: // "/" — head to the upper-right
            overlay(canvas[0], 4, "[##]");
            overlay(canvas[1], 4, "/");
            overlay(canvas[2], 3, "/");
            overlay(canvas[3], 2, "/");
            break;
        case 3: // "-" — head swung down flat onto the corner
            overlay(canvas[5], 5, "[##]");
            overlay(canvas[5], 1, "----");
            break;
        default:
            break;
    }

    switch (step.emberStage) {
        case 1: // burst — hot sparks right at the corner
            overlay(canvas[4], 7, "*");
            overlay(canvas[4], 10, "*");
            overlay(canvas[3], 9, ".");
            overlay(canvas[5], 11, ".");
            break;
        case 2: // drift — sparks risen and spread up-and-right, cooling
            overlay(canvas[3], 9, "*");
            overlay(canvas[2], 12, ".");
            overlay(canvas[4], 12, ".");
            overlay(canvas[3], 15, "`");
            break;
        case 3: // drift further — faint embers near the top, well to the right
            overlay(canvas[1], 12, ".");
            overlay(canvas[2], 15, "`");
            overlay(canvas[0], 14, "`");
            overlay(canvas[2], 18, "`");
            break;
        default:
            break;
    }

    // Colour the sparks in the animated header rows only; the logo block below is
    // static and left uncoloured.
    for (size_t row = 0; row < headerRows; ++row) {
        colorizeSparks(canvas[row]);
    }

    canvas[totalRows - 1] = fmt::format("  forging  {:.1f}/{:.1f}s", elapsedSeconds, totalSeconds);
}

// Repaint a multi-line frame in place: on every frame after the first, move the
// cursor up by the frame height, then clear and rewrite each line.
void renderFrame(const std::vector<std::string>& lines, bool firstFrame) {
    if (!firstFrame) {
        fmt::print("\033[{}A", lines.size());
    }
    for (const std::string& line : lines) {
        fmt::print("\r\033[K{}\n", line);
    }
    fflush(stdout);
}

// One frame of the compact single-line ("simple") animation: a hammer (T)
// striking an anvil (▟██▙) with sparks, repainted in place via carriage return.
void drawForgeFrame(size_t frame, double elapsedSeconds, double totalSeconds) {
    static const char* const FRAMES[] = {
        "T     ▟██▙          ",
        "   T  ▟██▙          ",
        "     T▟██▙   *  *   ",
        "   T  ▟██▙    *   * ",
    };
    constexpr size_t frameCount = sizeof(FRAMES) / sizeof(FRAMES[0]);

    fmt::print("\r  {}  forging  {:.1f}/{:.1f}s ",
               FRAMES[frame % frameCount],
               elapsedSeconds,
               totalSeconds);
    fflush(stdout);
}

}

ForgeSpinner::ForgeSpinner(Mode mode)
    : _mode(mode),
      _enabled(mode != Mode::NONE && isatty(STDOUT_FILENO) != 0) {
}

ForgeSpinner::Mode ForgeSpinner::parseMode(const std::string& value) {
    if (value == "none") {
        return Mode::NONE;
    } else if (value == "special") {
        return Mode::SPECIAL;
    }
    return Mode::SIMPLE;
}

void ForgeSpinner::run(TimePoint runStart, TimePoint deadline, double totalSeconds) {
    const bool multiLine = (_mode == Mode::SPECIAL);

    // Hide the cursor while repainting the multi-line scene; the guard restores it
    // on every exit path, including an exception thrown mid-frame.
    const CursorHider cursorHider(_enabled && multiLine);

    // Reused across frames so each repaint doesn't reallocate the line buffer.
    std::vector<std::string> lines;

    bool firstFrame = true;
    for (size_t frame = 0; Clock::now() < deadline; ++frame) {
        if (_enabled) {
            const double elapsed = std::chrono::duration<double>(Clock::now() - runStart).count();
            if (multiLine) {
                buildLogoFrame(frame, elapsed, totalSeconds, lines);
                renderFrame(lines, firstFrame);
                firstFrame = false;
            } else {
                drawForgeFrame(frame, elapsed, totalSeconds);
            }
        }
        std::this_thread::sleep_until(std::min(Clock::now() + std::chrono::milliseconds(150), deadline));
    }

    // The multi-line scene leaves the forged logo on screen (the cursor is
    // restored by cursorHider); the single-line animation is cleared before the
    // results are printed.
    if (_enabled && !multiLine) {
        fmt::print("\r\033[K");
        fflush(stdout);
    }
}
