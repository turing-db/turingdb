#include "ExplainRequest.h"

#include <ctype.h>
#include <algorithm>
#include <array>

using namespace db;

namespace {

bool equalsInsensitive(std::string_view left, std::string_view right) {
    const auto sameLetter = [](char leftChar, char rightChar) {
        return tolower(static_cast<unsigned char>(leftChar)) == tolower(static_cast<unsigned char>(rightChar));
    };

    return std::ranges::equal(left, right, sameLetter);
}

struct StageWord {
    std::string_view word;
    ExplainStage stage {ExplainStage::AST};
};

const std::array<StageWord, 6> stageWords {{
    {"ast", ExplainStage::AST},
    {"vdg", ExplainStage::VDG},
    {"codegen", ExplainStage::CODEGEN},
    {"passes", ExplainStage::PASSES},
    {"db", ExplainStage::DB},
    {"nl", ExplainStage::NL},
}};

bool contains(const std::vector<std::string_view>& passNames, std::string_view passName) {
    return std::ranges::any_of(passNames, [passName](std::string_view name) {
        return equalsInsensitive(name, passName);
    });
}

}

ExplainRequest::ExplainRequest() {
}

ExplainRequest::~ExplainRequest() {
}

void ExplainRequest::requestDefaults() {
    request(ExplainStage::DB);
    request(ExplainStage::NL);
}

void ExplainRequest::requestAll() {
    for (const StageWord& stageWord : stageWords) {
        request(stageWord.stage);
    }
}

bool ExplainRequest::requestStage(std::string_view word) {
    for (const StageWord& stageWord : stageWords) {
        if (equalsInsensitive(stageWord.word, word)) {
            request(stageWord.stage);
            return true;
        }
    }

    return false;
}

bool ExplainRequest::requestPass(std::string_view selector, std::string_view passName) {
    const bool isAround = equalsInsensitive(selector, "around");
    const bool printsBefore = isAround || equalsInsensitive(selector, "before");
    const bool printsAfter = isAround || equalsInsensitive(selector, "after");

    if (!printsBefore && !printsAfter) {
        return false;
    }

    if (printsBefore) {
        _passesBefore.push_back(passName);
    }

    if (printsAfter) {
        _passesAfter.push_back(passName);
    }

    return true;
}

bool ExplainRequest::isRequested(ExplainStage stage) const {
    return (_stages & (1u << static_cast<uint32_t>(stage))) != 0;
}

bool ExplainRequest::isPassPrintedBefore(std::string_view passName) const {
    return contains(_passesBefore, passName);
}

bool ExplainRequest::isPassPrintedAfter(std::string_view passName) const {
    return contains(_passesAfter, passName);
}

std::string_view ExplainRequest::findUnknownPass(std::span<const std::string_view> pipelinePasses) const {
    for (const std::vector<std::string_view>* named : {&_passesBefore, &_passesAfter}) {
        for (const std::string_view passName : *named) {
            const bool isInPipeline = std::ranges::any_of(pipelinePasses, [passName](std::string_view name) {
                return equalsInsensitive(name, passName);
            });

            if (!isInPipeline) {
                return passName;
            }
        }
    }

    return {};
}

std::string_view ExplainRequest::getStageName(ExplainStage stage) {
    switch (stage) {
        case ExplainStage::AST:
            return "ast";
        break;

        case ExplainStage::VDG:
            return "vdg";
        break;

        case ExplainStage::CODEGEN:
            return "codegen";
        break;

        case ExplainStage::PASSES:
            return "passes";
        break;

        case ExplainStage::DB:
            return "db";
        break;

        case ExplainStage::NL:
            return "nl";
        break;
    }

    return "";
}

void ExplainRequest::describeOptions(std::string_view word, std::string& message) {
    message = "Unknown EXPLAIN option '";
    message += word;
    message += "'. Expected one of ast, vdg, codegen, passes, db, nl, all, "
               "or before, after, around followed by a pass name";
}

void ExplainRequest::request(ExplainStage stage) {
    _stages |= 1u << static_cast<uint32_t>(stage);

    if (stage == ExplainStage::PASSES) {
        _stages |= 1u << static_cast<uint32_t>(ExplainStage::CODEGEN);
    }
}
