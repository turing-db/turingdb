#pragma once

#include <string>

#include "TuringTime.h"

namespace forge {

// Renders the TuringForge progress animation on the calling (main) thread while
// a benchmark run is in flight. All terminal control (escape codes, cursor
// hiding, repainting) lives here, so the caller only picks a mode and runs it.
class ForgeSpinner {
public:
    enum class Mode {
        NONE,    // no animation
        SIMPLE,  // compact single-line hammer-and-anvil spinner
        SPECIAL, // multi-line hammer striking the TuringDB logo, with embers
    };

    explicit ForgeSpinner(Mode mode);

    // Resolve the --animation argument value ("none" | "simple" | "special")
    // into a Mode; anything else falls back to SIMPLE.
    static Mode parseMode(const std::string& value);

    // Animate on the calling thread until `deadline`, redrawing at a fixed
    // cadence. The displayed elapsed time is measured from `runStart`, and
    // `totalSeconds` is the configured run length shown alongside it. When the
    // mode is NONE or stdout is not a terminal, this just sleeps until the
    // deadline without drawing.
    void run(TimePoint runStart, TimePoint deadline, double totalSeconds);

private:
    Mode _mode {Mode::SIMPLE};
    bool _enabled {false}; // mode != NONE and stdout is a TTY
};

}
