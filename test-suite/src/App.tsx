import React from "react";
import mermaid from "mermaid";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger
} from "@/components/ui/dialog";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInput,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
  useSidebar
} from "@/components/ui/sidebar";
import { PanelLeft } from "lucide-react";

type TestMeta = {
  name: string;
  enabled: boolean;
  query?: string;
  tags?: string[];
  writeRequired?: boolean;
};

type TestResult = {
  name: string;
  planOutput: string;
  resultOutput: string;
  planMatched: boolean;
  resultMatched: boolean;
  error?: string;
};

const API_BASE = "/api";
const SIDEBAR_MIN_WIDTH = 200;
const SIDEBAR_MAX_WIDTH = 820;

function sanitizeTestName(name: string) {
  return name.trim().replace(/[\\/]/g, "-").replace(/[^a-z0-9._-]+/gi, "-");
}

function SidebarEdgeTrigger() {
  const { state, toggleSidebar } = useSidebar();
  const isCollapsed = state === "collapsed";
  return (
    <button
      type="button"
      onClick={toggleSidebar}
      className="absolute left-0 top-6 z-40 flex items-center gap-2 rounded-r-full border border-sidebar-border bg-sidebar px-3 py-2 text-[10px] uppercase tracking-[0.2em] text-sidebar-foreground shadow-lg hover:border-accent/50"
      aria-label={isCollapsed ? "Open menu" : "Collapse menu"}
    >
      <PanelLeft className="h-4 w-4" />
      {isCollapsed ? "Menu" : "Hide"}
    </button>
  );
}

function SidebarResizeHandle({
  onStart
}: {
  onStart: (event: React.MouseEvent<HTMLDivElement>) => void;
}) {
  const { state } = useSidebar();
  if (state !== "expanded") return null;
  return (
    <div
      className="absolute inset-y-0 left-0 z-40 hidden w-3 -translate-x-1/2 cursor-col-resize md:block"
      onMouseDown={(event) => {
        event.preventDefault();
        onStart(event);
      }}
    />
  );
}

export default function App() {
  const [sidebarWidth, setSidebarWidth] = React.useState(420);
  const sidebarRef = React.useRef<HTMLDivElement>(null);
  const dragStartX = React.useRef(0);
  const dragStartWidth = React.useRef(0);
  const isDragging = React.useRef(false);
  const dragWidth = React.useRef(sidebarWidth);
  const dragRaf = React.useRef<number | null>(null);
  const [tests, setTests] = React.useState<TestMeta[]>([]);
  const [selected, setSelected] = React.useState<TestMeta | null>(null);
  const [results, setResults] = React.useState<Record<string, TestResult>>({});
  const [search, setSearch] = React.useState("");
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [confirmTarget, setConfirmTarget] = React.useState<"plan" | "result" | null>(null);
  const [nameDraft, setNameDraft] = React.useState("");
  const [queryDraft, setQueryDraft] = React.useState("");
  const [isEditingQuery, setIsEditingQuery] = React.useState(false);
  const [tagFilters, setTagFilters] = React.useState<string[]>([]);
  const [tagDraft, setTagDraft] = React.useState("");
  const [writeRequired, setWriteRequired] = React.useState(false);
  const [showPassing, setShowPassing] = React.useState(true);
  const [showFailing, setShowFailing] = React.useState(true);
  const [showDisabled, setShowDisabled] = React.useState(true);
  const [showNotRun, setShowNotRun] = React.useState(true);
  const [newTestOpen, setNewTestOpen] = React.useState(false);
  const [newTestName, setNewTestName] = React.useState("");
  const [planSvg, setPlanSvg] = React.useState<string | null>(null);
  const renderSeq = React.useRef(0);
  const [parsedResult, setParsedResult] = React.useState<string[][] | null>(null);

  const loadTests = React.useCallback(async (preferName?: string) => {
    try {
      const res = await fetch(`${API_BASE}/tests`);
      const data = (await res.json()) as TestMeta[];
      setTests(data);
      setSelected((prev) => {
        if (preferName) {
          const preferred = data.find((test) => test.name === preferName);
          if (preferred) return preferred;
        }
        if (!prev) return data[0] ?? null;
        const next = data.find((test) => test.name === prev.name);
        return next ?? data[0] ?? null;
      });
      return data;
    } catch {
      setError("Failed to load test list. Ensure the bun server is running.");
      return [];
    }
  }, []);

  React.useEffect(() => {
    loadTests();
  }, [loadTests]);
  
  React.useEffect(() => {
    setQueryDraft(selected?.query ?? "");
    setNameDraft(selected?.name ?? "");
    setTagDraft("");
    setWriteRequired(selected?.writeRequired ?? false);
    setIsEditingQuery(false);
  }, [selected]);

  const runTest = async (name: string) => {
    setLoading(true);
    setError(null);
    setResults((prev) => ({ ...prev }));
    try {
      const res = await fetch(`${API_BASE}/run?test=${encodeURIComponent(name)}`);
      if (!res.ok) {
        throw new Error("Runner API unavailable");
      }
      const data = (await res.json()) as TestResult;
      setResults((prev) => ({ ...prev, [data.name]: data }));
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
        next[entry.name] = entry;
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
      const payload: { name: string; plan?: string; result?: string } = { name: selected.name };
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
        [selected.name]: {
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
          name: selected.name,
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
          test.name === selected.name
            ? { ...test, query: queryDraft }
            : test
        )
      );
      setSelected((prev) =>
        prev ? { ...prev, query: queryDraft } : prev
      );
      await runTest(selected.name);
      setIsEditingQuery(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to update query.";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const updateName = async () => {
    if (!selected) return;
    const sanitized = sanitizeTestName(nameDraft);
    if (!sanitized) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/update`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: selected.name,
          newName: sanitized
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
          test.name === selected.name
            ? { ...test, name: sanitized }
            : test
        )
      );
      setSelected((prev) =>
        prev ? { ...prev, name: sanitized } : prev
      );
      setResults((prev) => {
        const next = { ...prev };
        const existing = next[selected.name];
        if (existing) {
          next[sanitized] = { ...existing, name: sanitized };
          delete next[selected.name];
        }
        return next;
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to update name.";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const updateTags = async (tags: string[]) => {
    if (!selected) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/update`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: selected.name,
          tags
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
          test.name === selected.name
            ? { ...test, tags }
            : test
        )
      );
      setSelected((prev) =>
        prev ? { ...prev, tags } : prev
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to update tags.";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const updateWriteRequired = async (nextValue: boolean) => {
    if (!selected) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/update`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: selected.name,
          writeRequired: nextValue
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
          test.name === selected.name
            ? { ...test, writeRequired: nextValue }
            : test
        )
      );
      setSelected((prev) =>
        prev ? { ...prev, writeRequired: nextValue } : prev
      );
      setWriteRequired(nextValue);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to update write mode.";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const updateEnabled = async (nextValue: boolean) => {
    if (!selected) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/update`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: selected.name,
          enabled: nextValue
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
          test.name === selected.name
            ? { ...test, enabled: nextValue }
            : test
        )
      );
      setSelected((prev) =>
        prev ? { ...prev, enabled: nextValue } : prev
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to update enabled state.";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const addTag = async () => {
    if (!selected) return;
    const nextTag = tagDraft.trim();
    if (!nextTag) return;
    const current = selected.tags ?? [];
    if (current.includes(nextTag)) {
      setTagDraft("");
      return;
    }
    await updateTags([...current, nextTag]);
    setTagDraft("");
  };

  const createTest = async () => {
    const name = sanitizeTestName(newTestName);
    if (!name) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/create`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name })
      });
      if (!res.ok) {
        const details = await res.json().catch(() => null);
        const message =
          typeof details?.error === "string"
            ? `${details.error}${details.details ? `: ${details.details}` : ""}`
            : "Failed to create test";
        throw new Error(message);
      }
      const payload = (await res.json()) as { name?: string };
      await loadTests(payload.name);
      setNewTestOpen(false);
      setNewTestName("");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to create test.";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const selectedResult = selected ? results[selected.name] : undefined;
  const allTags = React.useMemo(() => {
    const set = new Set<string>();
    for (const test of tests) {
      for (const tag of test.tags ?? []) {
        set.add(tag);
      }
    }
    return Array.from(set).sort();
  }, [tests]);
  const filteredTests = React.useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return tests;
    return tests.filter((test) => {
      const hay = `${test.name} ${test.query ?? ""}`.toLowerCase();
      return hay.includes(term);
    });
  }, [search, tests]);
  const visibleTests = React.useMemo(() => {
    let next = filteredTests;
    if (tagFilters.length > 0) {
      next = next.filter((test) => {
        const tags = test.tags ?? [];
        return tagFilters.every((tag) => tags.includes(tag));
      });
    }
    return next.filter((test) => {
      if (!test.enabled) return showDisabled;
      const testResult = results[test.name];
      if (!testResult) return showNotRun;
      const isPass = testResult.planMatched && testResult.resultMatched;
      return isPass ? showPassing : showFailing;
    });
  }, [filteredTests, tagFilters, results, showDisabled, showFailing, showNotRun, showPassing]);

  React.useEffect(() => {
    const onMove = (event: MouseEvent) => {
      if (!isDragging.current) return;
      const delta = event.clientX - dragStartX.current;
      const next = Math.min(
        SIDEBAR_MAX_WIDTH,
        Math.max(SIDEBAR_MIN_WIDTH, dragStartWidth.current + delta)
      );
      dragWidth.current = next;
      if (dragRaf.current === null) {
        dragRaf.current = window.requestAnimationFrame(() => {
          dragRaf.current = null;
          const node = sidebarRef.current;
          if (node) {
            node.style.setProperty("--sidebar-width", `${dragWidth.current}px`);
          }
        });
      }
    };
    const onUp = () => {
      if (isDragging.current) {
        isDragging.current = false;
        document.body.style.userSelect = "";
        setSidebarWidth(dragWidth.current);
      }
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      if (dragRaf.current !== null) {
        window.cancelAnimationFrame(dragRaf.current);
        dragRaf.current = null;
      }
    };
  }, []);

  React.useEffect(() => {
    mermaid.initialize({
      startOnLoad: false,
      theme: "dark",
      securityLevel: "strict",
      themeVariables: {
        primaryColor: "#0f172a",
        primaryTextColor: "#e2e8f0",
        lineColor: "#38bdf8",
        nodeBorder: "#38bdf8",
        edgeLabelBackground: "#0b0f14"
      }
    });
  }, []);

  React.useEffect(() => {
    const source = selectedResult?.planOutput?.trim();
    if (!source || (!source.startsWith("flowchart") && !source.startsWith("graph"))) {
      setPlanSvg(null);
      return;
    }
    let cancelled = false;
    const id = `plan-${renderSeq.current++}`;
    mermaid
      .render(id, source)
      .then((res) => {
        if (!cancelled) setPlanSvg(res.svg);
      })
      .catch(() => {
        if (!cancelled) setPlanSvg(null);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedResult?.planOutput]);

  React.useEffect(() => {
    const output = selectedResult?.resultOutput ?? "";
    const trimmed = output.trim();
    if (!trimmed) {
      setParsedResult(null);
      return;
    }
    if (/error/i.test(trimmed) || trimmed.startsWith("ANALYZE") || trimmed.startsWith("EXEC")) {
      setParsedResult(null);
      return;
    }
    const rows: string[][] = [];
    let current: string[] = [];
    let cell = "";
    let inQuotes = false;
    for (let i = 0; i < trimmed.length; i += 1) {
      const ch = trimmed[i];
      const next = trimmed[i + 1];
      if (inQuotes) {
        if (ch === "\"" && next === "\"") {
          cell += "\"";
          i += 1;
        } else if (ch === "\"") {
          inQuotes = false;
        } else {
          cell += ch;
        }
      } else if (ch === "\"") {
        inQuotes = true;
      } else if (ch === ",") {
        current.push(cell);
        cell = "";
      } else if (ch === "\n") {
        current.push(cell);
        rows.push(current);
        current = [];
        cell = "";
      } else {
        cell += ch;
      }
    }
    current.push(cell);
    rows.push(current);
    setParsedResult(rows);
  }, [selectedResult?.resultOutput]);

  return (
    <SidebarProvider ref={sidebarRef} defaultOpen sidebarWidth={sidebarWidth}>
      <Sidebar>
        <SidebarHeader className="gap-3 border-b border-sidebar-border px-4 py-4">
          <div className="flex items-center justify-between">
            <h2 className="text-base font-semibold">Available Tests</h2>
          </div>
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>{visibleTests.length} total</span>
            {tagFilters.length > 0 && (
              <button
                className="text-[10px] uppercase tracking-[0.16em] text-accent"
                onClick={() => setTagFilters([])}
              >
                Clear filters
              </button>
            )}
          </div>
          <SidebarInput
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search tests or query text (e.g., MATCH (n))…"
          />
          <div className="flex flex-wrap items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-muted-foreground">
            <label className="flex items-center gap-1">
              <input type="checkbox" checked={showPassing} onChange={(e) => setShowPassing(e.target.checked)} />
              Pass
            </label>
            <label className="flex items-center gap-1">
              <input type="checkbox" checked={showFailing} onChange={(e) => setShowFailing(e.target.checked)} />
              Fail
            </label>
            <label className="flex items-center gap-1">
              <input type="checkbox" checked={showNotRun} onChange={(e) => setShowNotRun(e.target.checked)} />
              Not Run
            </label>
            <label className="flex items-center gap-1">
              <input type="checkbox" checked={showDisabled} onChange={(e) => setShowDisabled(e.target.checked)} />
              Disabled
            </label>
          </div>
        </SidebarHeader>
        <SidebarContent>
          {allTags.length > 0 && (
            <SidebarGroup>
              <SidebarGroupLabel>Filter Tags</SidebarGroupLabel>
              <SidebarGroupContent className="flex flex-wrap gap-2">
                {allTags.map((tag) => {
                  const active = tagFilters.includes(tag);
                  return (
                    <button
                      key={tag}
                      onClick={() =>
                        setTagFilters((prev) =>
                          active ? prev.filter((t) => t !== tag) : [...prev, tag]
                        )
                      }
                      className={`rounded-full border px-2 py-1 text-[10px] uppercase tracking-[0.16em] ${
                        active
                          ? "border-accent/60 bg-accent/15 text-accent"
                          : "border-sidebar-border bg-sidebar-accent/40 text-muted-foreground hover:border-accent/30"
                      }`}
                    >
                      {tag}
                    </button>
                  );
                })}
              </SidebarGroupContent>
            </SidebarGroup>
          )}
          <SidebarGroup>
            <SidebarGroupLabel>Tests</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {visibleTests.map((test) => {
                  const testResult = results[test.name];
                  const isPending = !testResult;
                  const isDisabled = !test.enabled;
                  const isPass = !!testResult && testResult.planMatched && testResult.resultMatched;
                  const statusClass = isDisabled
                    ? "text-amber-400"
                    : isPass
                      ? "text-emerald-400"
                      : testResult
                        ? "text-red-400"
                        : "text-muted-foreground";
                  const badgeClass = isDisabled
                    ? "bg-amber-400/20 text-amber-100"
                    : isPass
                      ? "bg-emerald-400/15 text-emerald-200"
                      : testResult
                        ? "bg-red-500/20 text-red-200"
                        : "bg-white/5 text-muted-foreground";
                  const icon = isDisabled ? "■" : isPass ? "●" : testResult ? "●" : "○";
                  return (
                    <SidebarMenuItem key={test.name}>
                        <SidebarMenuButton
                          onClick={() => setSelected(test)}
                          isActive={selected?.name === test.name}
                          size="sm"
                          className="px-2"
                        >
                          <span className={`text-[10px] ${statusClass}`}>{icon}</span>
                          <span className="truncate text-xs font-medium text-sidebar-foreground">{test.name}</span>
                          <span
                            className={`ml-auto rounded-full px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] ${badgeClass}`}
                          >
                          {isDisabled ? "disabled" : isPending ? "pending" : isPass ? "pass" : "fail"}
                        </span>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  );
                })}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
      </Sidebar>

      <SidebarInset className="bg-transparent">
        <div className="relative flex min-h-svh w-full flex-col overflow-hidden px-6 py-10">
          <SidebarEdgeTrigger />
          <SidebarResizeHandle
            onStart={(event) => {
              isDragging.current = true;
              dragStartX.current = event.clientX;
              dragStartWidth.current = sidebarWidth;
              dragWidth.current = sidebarWidth;
              document.body.style.userSelect = "none";
            }}
          />
          <header className="mx-auto flex w-full max-w-6xl items-center justify-between">
            <div className="flex items-center gap-3">
              <div>
                <p className="text-xs uppercase tracking-[0.3em] text-moss">TuringDB</p>
                <h1 className="text-4xl font-semibold text-ink">Query Test Suite</h1>
              </div>
            </div>
            <div className="flex items-center gap-3">
              <Button variant="ghost" onClick={runAll} disabled={loading}>
                Run All
              </Button>
              <Dialog open={newTestOpen} onOpenChange={setNewTestOpen}>
                <DialogTrigger asChild>
                  <Button variant="ghost" disabled={loading}>
                    New Test
                  </Button>
                </DialogTrigger>
                <DialogContent className="surface border-white/10">
                  <DialogHeader>
                    <DialogTitle>Create new test</DialogTitle>
                    <DialogDescription>
                      Provide a unique test name. It will be used as the filename.
                    </DialogDescription>
                  </DialogHeader>
                  <div className="space-y-2">
                    <label className="text-xs uppercase tracking-[0.2em] text-ink/60">Test name</label>
                    <input
                      value={newTestName}
                      onChange={(event) => setNewTestName(event.target.value)}
                      placeholder="e.g. reads-match-basic"
                      className="w-full rounded-xl border border-white/10 bg-paper px-3 py-2 text-sm text-ink focus:border-accent/60 focus:outline-none"
                    />
                  </div>
                  <DialogFooter>
                    <Button variant="ghost" onClick={() => setNewTestOpen(false)}>
                      Cancel
                    </Button>
                    <Button variant="accent" onClick={createTest} disabled={loading || !newTestName.trim()}>
                      Create
                    </Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
              <Button variant="accent" onClick={() => selected && runTest(selected.name)} disabled={!selected || loading}>
                Run Selected
              </Button>
            </div>
          </header>

          <main className="mx-auto mt-8 flex w-full max-w-6xl flex-1 gap-6 overflow-hidden">
            <section className="surface min-w-0 flex-1 rounded-3xl p-6">
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold">Run Output</h2>
            <div className="flex items-center gap-2 text-xs text-ink/60">
              <span>Plan</span>
              <span>•</span>
              <span>Result</span>
            </div>
          </div>

          {selected && (
            <div className="mt-4 rounded-2xl border border-white/10 bg-steel/40 p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-ink/60">Selected</p>
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <input
                  value={nameDraft}
                  onChange={(event) => setNameDraft(event.target.value)}
                  className="flex-1 rounded-xl border border-white/10 bg-paper px-3 py-2 text-sm text-ink focus:border-accent/60 focus:outline-none"
                />
                <Button variant="ghost" size="sm" onClick={updateName} disabled={loading || !selected}>
                  Update Name
                </Button>
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                {(selected.tags ?? []).map((tag) => (
                  <button
                    key={tag}
                    onClick={() =>
                      updateTags((selected.tags ?? []).filter((entry) => entry !== tag))
                    }
                    className="rounded-full border border-white/10 bg-steel/60 px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] text-ink/70 hover:border-accent/40"
                    title="Remove tag"
                  >
                    {tag}
                  </button>
                ))}
              </div>
              <div className="mt-3 flex items-center gap-2">
                <input
                  value={tagDraft}
                  onChange={(event) => setTagDraft(event.target.value)}
                  placeholder="Add tag"
                  className="flex-1 rounded-xl border border-white/10 bg-paper px-3 py-2 text-xs text-ink focus:border-accent/60 focus:outline-none"
                />
                <Button variant="ghost" size="sm" onClick={addTag} disabled={loading || !selected}>
                  Add Tag
                </Button>
              </div>
              <label className="mt-4 flex items-center gap-2 text-xs text-ink/70">
                <input
                  type="checkbox"
                  checked={writeRequired}
                  onChange={(event) => updateWriteRequired(event.target.checked)}
                  className="h-4 w-4 rounded border border-white/20 bg-paper text-accent focus:ring-2 focus:ring-accent/40"
                />
                Require write transaction
              </label>
              <label className="mt-2 flex items-center gap-2 text-xs text-ink/70">
                <input
                  type="checkbox"
                  checked={selected.enabled}
                  onChange={(event) => updateEnabled(event.target.checked)}
                  className="h-4 w-4 rounded border border-white/20 bg-paper text-accent focus:ring-2 focus:ring-accent/40"
                />
                Enabled
              </label>
            </div>
          )}
          {selected && (
            <div className="mt-4 rounded-2xl border border-white/10 bg-steel/40 p-4">
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
                    className="mt-2 h-28 w-full resize-none rounded-xl border border-white/10 bg-paper p-3 text-xs font-mono text-ink focus:border-accent/60 focus:outline-none"
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
            <div className="mt-6 rounded-2xl border border-white/10 bg-steel/40 p-4">
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
              <div className="rounded-2xl border border-white/10 bg-steel/40 p-4">
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
                {parsedResult && parsedResult.length > 0 ? (
                  <div className="mt-3 max-h-[48rem] overflow-auto rounded-xl border border-white/5 bg-paper">
                    <table className="w-full border-collapse text-left text-xs text-ink">
                      <thead className="sticky top-0 bg-steel/60 text-ink/80">
                        <tr>
                          {parsedResult[0].map((cell, idx) => (
                            <th key={idx} className="border-b border-white/5 px-3 py-2 font-semibold">
                              {cell}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {parsedResult.slice(1).map((row, rowIdx) => (
                          <tr key={rowIdx} className="odd:bg-white/5">
                            {row.map((cell, cellIdx) => (
                              <td key={cellIdx} className="border-b border-white/5 px-3 py-2">
                                {cell}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <pre className="mt-3 max-h-[48rem] overflow-auto whitespace-pre-wrap rounded-xl bg-paper p-3 text-xs font-mono text-ink">
{selectedResult.resultOutput || "(empty)"}
                  </pre>
                )}
              </div>
              <div className="rounded-2xl border border-white/10 bg-steel/40 p-4">
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
                {planSvg ? (
                  <div
                    className="mt-3 max-h-[36rem] overflow-auto rounded-xl bg-paper p-3"
                    dangerouslySetInnerHTML={{ __html: planSvg }}
                  />
                ) : (
                  <pre className="mt-3 max-h-[36rem] overflow-auto whitespace-pre-wrap rounded-xl bg-paper p-3 text-xs font-mono text-ink">
{selectedResult.planOutput || "(empty)"}
                  </pre>
                )}
              </div>
            </div>
          )}

          {(!selected || !selectedResult) && !loading && !error && (
            <div className="mt-6 rounded-2xl border border-white/10 bg-steel/40 p-4 text-sm text-ink/70">
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
                <span className="font-semibold text-ink">{selected?.name}</span>.
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
      </SidebarInset>
    </SidebarProvider>
  );
}
