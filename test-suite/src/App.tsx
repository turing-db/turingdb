import React from "react";
import { Button } from "./components/ui/button";

type TestMeta = {
  id: string;
  name: string;
  enabled: boolean;
  query?: string;
};

type TestResult = {
  id: string;
  planOutput: string;
  resultOutput: string;
  planMatched: boolean;
  resultMatched: boolean;
  error?: string;
};

const API_BASE = "/api";

export default function App() {
  const [tests, setTests] = React.useState<TestMeta[]>([]);
  const [selected, setSelected] = React.useState<TestMeta | null>(null);
  const [results, setResults] = React.useState<Record<string, TestResult>>({});
  const [search, setSearch] = React.useState("");
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [confirmTarget, setConfirmTarget] = React.useState<"plan" | "result" | null>(null);
  const [queryDraft, setQueryDraft] = React.useState("");
  const [isEditingQuery, setIsEditingQuery] = React.useState(false);

  React.useEffect(() => {
    fetch(`${API_BASE}/tests`)
      .then((res) => res.json())
      .then((data) => {
        setTests(data);
        setSelected(data[0] ?? null);
        setQueryDraft(data[0]?.query ?? "");
      })
      .catch(() => {
        setError("Failed to load test list. Ensure the bun server is running.");
      });
  }, []);
  
  React.useEffect(() => {
    setQueryDraft(selected?.query ?? "");
    setIsEditingQuery(false);
  }, [selected]);

  const runTest = async (id: string) => {
    setLoading(true);
    setError(null);
    setResults((prev) => ({ ...prev }));
    try {
      const res = await fetch(`${API_BASE}/run?test=${encodeURIComponent(id)}`);
      if (!res.ok) {
        throw new Error("Runner API unavailable");
      }
      const data = (await res.json()) as TestResult;
      setResults((prev) => ({ ...prev, [data.id]: data }));
    } catch (err) {
      setError("Runner API unavailable. Build and run query_test_suite_cli to wire this up.");
    } finally {
      setLoading(false);
    }
  };

  const runAll = async () => {
    setLoading(true);
    setError(null);
    setResults((prev) => ({ ...prev }));
    try {
      const res = await fetch(`${API_BASE}/run-all`);
      if (!res.ok) {
        throw new Error("Runner API unavailable");
      }
      const data = (await res.json()) as TestResult[];
      const next: Record<string, TestResult> = {};
      for (const entry of data) {
        next[entry.id] = entry;
      }
      setResults(next);
    } catch (err) {
      setError("Runner API unavailable. Build and run query_test_suite_cli to wire this up.");
    } finally {
      setLoading(false);
    }
  };

  const acceptOutputs = async (target: "plan" | "result") => {
    if (!selected || !selectedResult) return;
    setLoading(true);
    setError(null);
    try {
      const payload: { id: string; plan?: string; result?: string } = { id: selected.id };
      if (target === "plan") payload.plan = selectedResult.planOutput;
      if (target === "result") payload.result = selectedResult.resultOutput;
      const res = await fetch(`${API_BASE}/update`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      if (!res.ok) {
        const details = await res.json().catch(() => null);
        const message =
          typeof details?.error === "string"
            ? `${details.error}${details.details ? `: ${details.details}` : ""}`
            : "Failed to update test";
        throw new Error(message);
      }
      setResults((prev) => ({
        ...prev,
        [selected.id]: {
          ...selectedResult,
          planMatched: target === "plan" ? true : selectedResult.planMatched,
          resultMatched: target === "result" ? true : selectedResult.resultMatched
        }
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to update test JSON.";
      setError(message);
    } finally {
      setLoading(false);
      setConfirmTarget(null);
    }
  };

  const updateQuery = async () => {
    if (!selected) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/update`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: selected.id,
          query: queryDraft
        })
      });
      if (!res.ok) {
        const details = await res.json().catch(() => null);
        const message =
          typeof details?.error === "string"
            ? `${details.error}${details.details ? `: ${details.details}` : ""}`
            : "Failed to update test";
        throw new Error(message);
      }
      setTests((prev) =>
        prev.map((test) =>
          test.id === selected.id
            ? { ...test, query: queryDraft }
            : test
        )
      );
      setSelected((prev) =>
        prev ? { ...prev, query: queryDraft } : prev
      );
      await runTest(selected.id);
      setIsEditingQuery(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to update query.";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const selectedResult = selected ? results[selected.id] : undefined;
  const filteredTests = React.useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return tests;
    return tests.filter((test) => {
      const hay = `${test.name} ${test.id} ${test.query ?? ""}`.toLowerCase();
      return hay.includes(term);
    });
  }, [search, tests]);

  return (
    <div className="flex h-screen flex-col overflow-hidden px-6 py-10">
      <header className="mx-auto flex w-full max-w-6xl items-center justify-between">
        <div>
          <p className="text-xs uppercase tracking-[0.3em] text-moss">TuringDB</p>
          <h1 className="text-4xl font-semibold text-ink">
            Query Test Suite
          </h1>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="ghost" onClick={runAll} disabled={loading}>
            Run All
          </Button>
          <Button variant="accent" onClick={() => selected && runTest(selected.id)} disabled={!selected || loading}>
            Run Selected
          </Button>
        </div>
      </header>

      <main className="mx-auto mt-8 grid w-full max-w-6xl flex-1 gap-6 overflow-hidden lg:grid-cols-[1.1fr_1.6fr]">
        <section className="surface flex min-h-0 flex-col overflow-hidden rounded-3xl p-6">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">Available Tests</h2>
            <span className="text-xs text-ink/60">{filteredTests.length} total</span>
          </div>
          <div className="mt-4">
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search tests or query text (e.g., MATCH (n))…"
              className="w-full rounded-2xl border border-black/10 bg-white/80 px-4 py-2 text-sm text-ink placeholder:text-ink/40 focus:border-moss/40 focus:outline-none"
            />
          </div>
          <div className="mt-4 min-h-0 flex-1 space-y-1.5 overflow-y-auto pr-2">
            {filteredTests.map((test) => (
              (() => {
                const testResult = results[test.id];
                const isPending = !testResult;
                const isPass = !!testResult && testResult.planMatched && testResult.resultMatched;
                const statusClass = isPass
                  ? "text-emerald-600"
                  : testResult
                    ? "text-red-500"
                    : "text-ink/40";
                const badgeClass = isPass
                  ? "bg-emerald-600/15 text-emerald-700"
                  : testResult
                    ? "bg-red-500/20 text-red-600"
                    : "bg-black/5 text-ink/40";
                const icon = isPass ? "●" : testResult ? "●" : "○";
                return (
              <button
                key={test.id}
                onClick={() => setSelected(test)}
                className={`w-full rounded-xl border px-2.5 py-1.5 text-left transition ${
                  selected?.id === test.id
                    ? "border-accent bg-accent/10"
                    : "border-black/10 bg-white/60 hover:border-ink/20"
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className={`text-[10px] ${statusClass}`}>{icon}</span>
                  <span className="truncate text-sm font-medium text-ink">{test.name}</span>
                  <span className="text-[10px] text-ink/40">•</span>
                  <span className="truncate text-[11px] text-ink/50">{test.id}</span>
                  <span className={`ml-auto rounded-full px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] ${badgeClass}`}>
                    {isPending ? "pending" : isPass ? "pass" : "fail"}
                  </span>
                </div>
              </button>
                );
              })()
            ))}
          </div>
        </section>

        <section className="surface rounded-3xl p-6">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">Run Output</h2>
            <div className="flex items-center gap-2 text-xs text-ink/60">
              <span>Plan</span>
              <span>•</span>
              <span>Result</span>
            </div>
          </div>

          {selected && (
            <div className="mt-4 rounded-2xl border border-black/10 bg-white/70 p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-ink/60">Selected</p>
              <p className="mt-1 text-lg font-semibold text-ink">{selected.name}</p>
              <p className="text-xs text-ink/60">{selected.id}</p>
            </div>
          )}
          {selected && (
            <div className="mt-4 rounded-2xl border border-black/10 bg-white/70 p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-ink/60">Query</p>
              {!isEditingQuery ? (
                <pre
                  onClick={() => setIsEditingQuery(true)}
                  className="mt-2 cursor-text whitespace-pre-wrap rounded-xl border border-transparent bg-paper p-3 text-xs font-mono text-ink"
                >
{selected.query ?? "(query not loaded)"}
                </pre>
              ) : (
                <>
                  <textarea
                    value={queryDraft}
                    onChange={(event) => setQueryDraft(event.target.value)}
                    className="mt-2 h-28 w-full resize-none rounded-xl border border-black/10 bg-paper p-3 text-xs font-mono text-ink focus:border-moss/40 focus:outline-none"
                    placeholder="(query not loaded)"
                  />
                  <div className="mt-3 flex items-center justify-end gap-2">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        setQueryDraft(selected.query ?? "");
                        setIsEditingQuery(false);
                      }}
                      disabled={loading}
                    >
                      Cancel
                    </Button>
                    <Button
                      variant="accent"
                      size="sm"
                      onClick={updateQuery}
                      disabled={loading || !selected}
                    >
                      Update Query
                    </Button>
                  </div>
                </>
              )}
            </div>
          )}

          {loading && (
            <div className="mt-6 rounded-2xl border border-black/10 bg-white/70 p-4">
              <p className="text-sm text-ink/70">Running tests…</p>
            </div>
          )}

          {error && (
            <div className="mt-6 rounded-2xl border border-accent/40 bg-accent/10 p-4 text-sm text-ink">
              {error}
            </div>
          )}

          {selected && selectedResult && (
            <div className="mt-6 grid gap-4">
              <div className="rounded-2xl border border-black/10 bg-white/80 p-4">
                <div className="flex items-center justify-between">
                  <p className="text-xs uppercase tracking-[0.2em] text-ink/60">Plan Output</p>
                  <div className="flex items-center gap-2">
                    <span
                      className={`rounded-full px-2 py-1 text-[10px] uppercase tracking-[0.18em] ${
                        selectedResult.planMatched
                          ? "bg-moss/15 text-moss"
                          : "bg-accent/15 text-accent"
                      }`}
                    >
                      {selectedResult.planMatched ? "match" : "mismatch"}
                    </span>
                    {!selectedResult.planMatched && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setConfirmTarget("plan")}
                        disabled={loading}
                      >
                        Accept
                      </Button>
                    )}
                  </div>
                </div>
                <pre className="mt-3 max-h-64 overflow-auto whitespace-pre-wrap rounded-xl bg-paper p-3 text-xs font-mono text-ink">
{selectedResult.planOutput || "(empty)"}
                </pre>
              </div>
              <div className="rounded-2xl border border-black/10 bg-white/80 p-4">
                <div className="flex items-center justify-between">
                  <p className="text-xs uppercase tracking-[0.2em] text-ink/60">Result Output</p>
                  <div className="flex items-center gap-2">
                    <span
                      className={`rounded-full px-2 py-1 text-[10px] uppercase tracking-[0.18em] ${
                        selectedResult.resultMatched
                          ? "bg-moss/15 text-moss"
                          : "bg-accent/15 text-accent"
                      }`}
                    >
                      {selectedResult.resultMatched ? "match" : "mismatch"}
                    </span>
                    {!selectedResult.resultMatched && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setConfirmTarget("result")}
                        disabled={loading}
                      >
                        Accept
                      </Button>
                    )}
                  </div>
                </div>
                <pre className="mt-3 max-h-64 overflow-auto whitespace-pre-wrap rounded-xl bg-paper p-3 text-xs font-mono text-ink">
{selectedResult.resultOutput || "(empty)"}
                </pre>
              </div>
            </div>
          )}

          {(!selected || !selectedResult) && !loading && !error && (
            <div className="mt-6 rounded-2xl border border-black/10 bg-white/70 p-4 text-sm text-ink/70">
              Connect the C++ runner API to populate plan/result output.
            </div>
          )}
        </section>
      </main>
      {confirmTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30">
          <div className="surface w-full max-w-md rounded-3xl p-6">
            <h3 className="text-lg font-semibold">Update test expectations?</h3>
            <p className="mt-2 text-sm text-ink/70">
              This will overwrite the expected {confirmTarget} in the JSON file for{" "}
              <span className="font-semibold text-ink">{selected?.id}</span>.
            </p>
            <div className="mt-6 flex items-center justify-end gap-3">
              <Button variant="ghost" onClick={() => setConfirmTarget(null)}>
                Cancel
              </Button>
              <Button variant="accent" onClick={() => acceptOutputs(confirmTarget)}>
                Confirm Update
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
