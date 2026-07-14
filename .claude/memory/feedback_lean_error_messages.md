Keep user-facing error/exception messages lean and factual. Two specific rules the user enforced when reviewing a `PipelineException` message for the data-part cap:

- **Do not advertise the config/env override knob in the message.** The user removed a trailing sentence that told the reader how to change the limit via the `TURING_MAX_DATAPARTS` environment variable. For a commercial guardrail especially, don't hand users the bypass in the error text.
- **Drop filler adjectives.** The user removed the word "configured" from "reaching the configured limit of {}" — just say "reaching the limit of {}".

**Why:** The message is a hard stop meant to steer the user to the intended remedy (here: run `MERGE_DATAPARTS`), not a tutorial on how to disable the guardrail or an editorialised sentence.

**How to apply:** State the situation and the concrete remedy; stop there. No env-var/config hints, no "configured"/"the specified"/similar qualifiers. Env-var overrides belong in code/docs, not in the thrown message.
