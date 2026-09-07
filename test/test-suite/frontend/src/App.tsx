import React from "react";
import mermaid from "mermaid";
import { JsonView, darkStyles } from "react-json-view-lite";
import "react-json-view-lite/dist/index.css";
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
import { AlertTriangle, Ban, Bug, CheckCircle2, Clock3, Copy, FilePlus, GitCompareArrows, PanelLeft, Play, PlayCircle, Plus, Share2, Trash2 } from "lucide-react";

type TestMeta = {
  name: string;
  enabled: boolean;
  remoteEnabled?: boolean;
  remoteDisabledReason?: string;
  query?: string;
  tags?: string[];
  writeRequired?: boolean;
  disabledReason?: string;
  mainVersion?: {
    query?: string;
    expect?: { plan?: string; result?: string; resultJson?: string; mlir?: string };
    tags?: string[];
    enabled?: boolean;
    ["write-required"]?: boolean;
    ["disabled-reason"]?: string;
  };
  changed?: boolean;
  isNew?: boolean;
};

type TestResult = {
  name: string;
  planOutput: string;
  resultOutput: string;
  resultJsonOutput?: string;
  resultJsonMatched?: boolean;
  resultJsonValid?: boolean;
  planMatched: boolean;
  resultMatched: boolean;
  error?: string;
  timeUs?: number;
};

type V3TestResult = {
  name: string;
  resultV3Output: string;
  mlirProgram: string;
  resultV3Matched: boolean;
  mlirMatched: boolean;
  error?: string;
  timeUs?: number;
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
  const [remoteResults, setRemoteResults] = React.useState<Record<string, TestResult>>({});
  const [v3Results, setV3Results] = React.useState<Record<string, V3TestResult>>({});
  const [search, setSearch] = React.useState("");
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [confirmTarget, setConfirmTarget] = React.useState<"plan" | "result" | "resultJson" | "mlir" | null>(null);
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
  const [deleteOpen, setDeleteOpen] = React.useState(false);
  const [duplicateOpen, setDuplicateOpen] = React.useState(false);
  const [planSvg, setPlanSvg] = React.useState<string | null>(null);
  const [planSvgExpected, setPlanSvgExpected] = React.useState<string | null>(null);
  const [planSvgMain, setPlanSvgMain] = React.useState<string | null>(null);
  const renderSeq = React.useRef(0);
  const [parsedResult, setParsedResult] = React.useState<string[][] | null>(null);
  const [parsedRemoteResult, setParsedRemoteResult] = React.useState<string[][] | null>(null);
  const [parsedExpectedResult, setParsedExpectedResult] = React.useState<string[][] | null>(null);
  const [parsedMainResult, setParsedMainResult] = React.useState<string[][] | null>(null);
  const [parsedV3Result, setParsedV3Result] = React.useState<string[][] | null>(null);
  const [disabledReasonDraft, setDisabledReasonDraft] = React.useState("");
  const [shareNotice, setShareNotice] = React.useState<string | null>(null);
  const shareTimerRef = React.useRef<number | null>(null);
  const [failNotice, setFailNotice] = React.useState<string | null>(null);
  const failTimerRef = React.useRef<number | null>(null);
  const [expectedPlan, setExpectedPlan] = React.useState<string>("");
  const [expectedResult, setExpectedResult] = React.useState<string>("");
  const [expectedResultJson, setExpectedResultJson] = React.useState<string>("");
  const [expectedMlir, setExpectedMlir] = React.useState<string>("");
  const [mainPlan, setMainPlan] = React.useState<string>("");
  const [mainResult, setMainResult] = React.useState<string>("");
  const [mainResultJson, setMainResultJson] = React.useState<string>("");
  const [mainMlir, setMainMlir] = React.useState<string>("");
  const [resultTab, setResultTab] = React.useState<"actual" | "expected" | "main">("actual");
  const [planTab, setPlanTab] = React.useState<"actual" | "expected" | "main">("actual");
  const [jsonTab, setJsonTab] = React.useState<"actual" | "expected" | "main">("actual");
  const [remoteResultTab, setRemoteResultTab] = React.useState<"actual" | "expected" | "main">("actual");
  const [resultV3Tab, setResultV3Tab] = React.useState<"actual" | "expected" | "main">("actual");
  const [mlirTab, setMlirTab] = React.useState<"actual" | "expected" | "main">("actual");

  const loadTests = React.useCallback(async (preferName?: string) => {
    try {
      const res = await fetch(`${API_BASE}/tests`);
      if (!res.ok) {
        const details = await res.json().catch(() => null);
        const message =
          typeof details?.error === "string"
            ? `${details.error}${details.details ? `: ${details.details}` : ""}`
            : "Failed to load test list";
        throw new Error(message);
      }
      const data = (await res.json()) as TestMeta[];
      setTests(data);
      setSelected((prev) => {
        const urlName = new URLSearchParams(window.location.search).get("test");
        if (urlName) {
          const preferred = data.find((test) => test.name === urlName);
          if (preferred) return preferred;
        }
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
    setDisabledReasonDraft(selected?.disabledReason ?? "");
    setIsEditingQuery(false);
    setResultTab("actual");
    setPlanTab("actual");
    setJsonTab("actual");
    setRemoteResultTab("actual");
    setResultV3Tab("actual");
    setMlirTab("actual");
  }, [selected]);

  React.useEffect(() => {
    if (!selected) {
      setExpectedPlan("");
      setExpectedResult("");
      setExpectedResultJson("");
      setExpectedMlir("");
      setMainPlan("");
      setMainResult("");
      setMainResultJson("");
      setMainMlir("");
      return;
    }
    let active = true;
    fetch(`${API_BASE}/expected?name=${encodeURIComponent(selected.name)}`)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (!active) return;
        setExpectedPlan(typeof data?.plan === "string" ? data.plan : "");
        setExpectedResult(typeof data?.result === "string" ? data.result : "");
        setExpectedResultJson(typeof data?.resultJson === "string" ? data.resultJson : "");
        setExpectedMlir(typeof data?.mlir === "string" ? data.mlir : "");
      })
      .catch(() => {
        if (!active) return;
        setExpectedPlan("");
        setExpectedResult("");
        setExpectedResultJson("");
        setExpectedMlir("");
      });
    const mainExpect = selected.mainVersion?.expect ?? {};
    if (typeof mainExpect.plan === "string" || typeof mainExpect.result === "string") {
      setMainPlan(typeof mainExpect.plan === "string" ? mainExpect.plan : "");
      setMainResult(typeof mainExpect.result === "string" ? mainExpect.result : "");
      setMainResultJson(typeof mainExpect.resultJson === "string" ? mainExpect.resultJson : "");
      setMainMlir(typeof mainExpect.mlir === "string" ? mainExpect.mlir : "");
    } else {
      fetch(`${API_BASE}/main?name=${encodeURIComponent(selected.name)}`)
        .then((res) => (res.ok ? res.json() : null))
        .then((data) => {
          if (!active) return;
          setMainPlan(typeof data?.plan === "string" ? data.plan : "");
          setMainResult(typeof data?.result === "string" ? data.result : "");
          setMainResultJson(typeof data?.resultJson === "string" ? data.resultJson : "");
          setMainMlir(typeof data?.mlir === "string" ? data.mlir : "");
        })
        .catch(() => {
          if (!active) return;
          setMainPlan("");
          setMainResult("");
          setMainResultJson("");
          setMainMlir("");
        });
    }
    return () => {
      active = false;
    };
  }, [selected?.name]);

  React.useEffect(() => {
    if (!selected) return;
    const url = new URL(window.location.href);
    url.searchParams.set("test", selected.name);
    window.history.replaceState({}, "", url.toString());
  }, [selected?.name]);

  const isLocalPass = React.useCallback(
    (result: TestResult) =>
      result.planMatched && result.resultMatched && result.resultJsonMatched === true,
    []
  );

  const isRemotePass = React.useCallback(
    (result: TestResult) =>
      result.resultMatched === true,
    []
  );

  // A test is only a v3 pass when the IR output reproduces the v2 output
  // (expect.result). The MLIR program match is a separate, informational signal
  // shown in its own panel and does not gate the overall pass/fail.
  const isV3Pass = React.useCallback(
    (result: V3TestResult) =>
      result.resultV3Matched === true,
    []
  );

  const readApiErrorMessage = React.useCallback(async (res: Response, fallback: string) => {
    const payload = (await res.json().catch(() => null)) as { error?: unknown; details?: unknown } | null;
    if (typeof payload?.error === "string" && payload.error.trim()) {
      if (typeof payload.details === "string" && payload.details.trim()) {
        return `${payload.error}: ${payload.details}`;
      }
      return payload.error;
    }
    return fallback;
  }, []);

  const fetchApiJson = React.useCallback(
    async <T,>(url: string, fallback: string): Promise<T> => {
      let res: Response;
      try {
        res = await fetch(url);
      } catch (err) {
        const message = err instanceof Error && err.message ? err.message : "Unable to reach runner backend";
        throw new Error(`Failed to reach runner backend: ${message}`);
      }
      if (!res.ok) {
        throw new Error(await readApiErrorMessage(res, fallback));
      }
      return (await res.json()) as T;
    },
    [readApiErrorMessage]
  );

  const runTest = async (name: string) => {
    setLoading(true);
    setError(null);
    setResults((prev) => ({ ...prev }));
    try {
      const [localData, v3Data] = await Promise.all([
        fetchApiJson<TestResult>(
          `${API_BASE}/run?test=${encodeURIComponent(name)}`,
          "Failed to run test"
        ),
        fetchApiJson<V3TestResult>(
          `${API_BASE}/run-v3?test=${encodeURIComponent(name)}`,
          "Failed to run v3 test"
        )
      ]);
      setResults((prev) => ({ ...prev, [localData.name]: localData }));
      setV3Results((prev) => ({ ...prev, [v3Data.name]: v3Data }));
      if (!isLocalPass(localData)) {
        showFailToast("1 test failed");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to run test");
    } finally {
      setLoading(false);
    }
  };

  const runAll = async () => {
    setLoading(true);
    setError(null);
    setResults((prev) => ({ ...prev }));
    setRemoteResults((prev) => ({ ...prev }));
    setV3Results((prev) => ({ ...prev }));
    try {
      const [localData, remoteData, v3Data] = await Promise.all([
        fetchApiJson<TestResult[]>(`${API_BASE}/run-all`, "Failed to run all tests"),
        fetchApiJson<TestResult[]>(`${API_BASE}/run-all-remote`, "Failed to run all remote tests"),
        fetchApiJson<V3TestResult[]>(`${API_BASE}/run-all-v3`, "Failed to run all v3 tests")
      ]);
      const nextLocal: Record<string, TestResult> = {};
      for (const entry of localData) {
        nextLocal[entry.name] = entry;
      }
      const nextRemote: Record<string, TestResult> = {};
      for (const entry of remoteData) {
        nextRemote[entry.name] = entry;
      }
      const nextV3: Record<string, V3TestResult> = {};
      for (const entry of v3Data) {
        nextV3[entry.name] = entry;
      }
      setResults(nextLocal);
      setRemoteResults(nextRemote);
      setV3Results(nextV3);
      const localFailed = localData.filter((entry) => !isLocalPass(entry)).length;
      const remoteFailed = remoteData.filter((entry) => !isRemotePass(entry)).length;
      const v3Failed = v3Data.filter((entry) => !isV3Pass(entry)).length;
      const notices: string[] = [];
      if (localFailed > 0) {
        notices.push(`${localFailed} local test${localFailed === 1 ? "" : "s"} failed`);
      }
      if (remoteFailed > 0) {
        notices.push(`${remoteFailed} remote test${remoteFailed === 1 ? "" : "s"} failed`);
      }
      if (v3Failed > 0) {
        notices.push(`${v3Failed} v3 test${v3Failed === 1 ? "" : "s"} failed`);
      }
      if (notices.length > 0) {
        showFailToast(notices.join(" / "));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to run test suite");
    } finally {
      setLoading(false);
    }
  };

  const acceptOutputs = async (target: "plan" | "result" | "resultJson" | "mlir") => {
    if (!selected) return;
    const isV3Target = target === "mlir";
    const localResult = selectedResult;
    const v3Result = selectedV3Result;
    if (isV3Target ? !v3Result : !localResult) return;
    setLoading(true);
    setError(null);
    try {
      const payload: {
        name: string;
        plan?: string;
        result?: string;
        resultJson?: string;
        mlir?: string;
      } = { name: selected.name };
      if (target === "plan" && localResult) payload.plan = localResult.planOutput;
      if (target === "result" && localResult) payload.result = localResult.resultOutput;
      if (target === "resultJson" && localResult) payload.resultJson = localResult.resultJsonOutput;
      if (target === "mlir" && v3Result) payload.mlir = v3Result.mlirProgram;
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
      if (isV3Target && v3Result) {
        setV3Results((prev) => ({
          ...prev,
          [selected.name]: {
            ...v3Result,
            mlirMatched: target === "mlir" ? true : v3Result.mlirMatched
          }
        }));
        if (target === "mlir") {
          setExpectedMlir(v3Result.mlirProgram);
        }
      } else if (localResult) {
        setResults((prev) => ({
          ...prev,
          [selected.name]: {
            ...localResult,
            planMatched: target === "plan" ? true : localResult.planMatched,
            resultMatched: target === "result" ? true : localResult.resultMatched,
            resultJsonMatched: target === "resultJson" ? true : localResult.resultJsonMatched
          }
        }));
        if (target === "resultJson") {
          setExpectedResultJson(localResult.resultJsonOutput ?? "");
        }
      }
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

  const updateDisabledReason = async () => {
    if (!selected) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/update`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: selected.name,
          disabledReason: disabledReasonDraft
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
            ? { ...test, disabledReason: disabledReasonDraft }
            : test
        )
      );
      setSelected((prev) =>
        prev ? { ...prev, disabledReason: disabledReasonDraft } : prev
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to update disabled reason.";
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

  const duplicateSelectedTest = async () => {
    if (!selected) return;
    setDuplicateOpen(true);
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/duplicate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: selected.name })
      });
      if (!res.ok) {
        const details = await res.json().catch(() => null);
        const message =
          typeof details?.error === "string"
            ? `${details.error}${details.details ? `: ${details.details}` : ""}`
            : "Failed to duplicate test";
        throw new Error(message);
      }
      const payload = (await res.json()) as { name?: string };
      const loaded = await loadTests(payload.name);
      if (payload.name) {
        const created = loaded.find((test) => test.name === payload.name);
        if (created) {
          setSelected(created);
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to duplicate test.";
      setError(message);
    } finally {
      setLoading(false);
      setDuplicateOpen(false);
    }
  };

  const deleteTest = async () => {
    if (!selected) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/delete`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: selected.name })
      });
      if (!res.ok) {
        const details = await res.json().catch(() => null);
        const message =
          typeof details?.error === "string"
            ? `${details.error}${details.details ? `: ${details.details}` : ""}`
            : "Failed to delete test";
        throw new Error(message);
      }
      setTests((prev) => prev.filter((test) => test.name !== selected.name));
      setResults((prev) => {
        const next = { ...prev };
        delete next[selected.name];
        return next;
      });
      setSelected((prev) => {
        if (!prev) return prev;
        const remaining = tests.filter((test) => test.name !== prev.name);
        return remaining[0] ?? null;
      });
      setDeleteOpen(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to delete test.";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const shareSelected = async () => {
    if (!selected) return;
    const url = new URL(window.location.href);
    url.searchParams.set("test", selected.name);
    try {
      await navigator.clipboard.writeText(url.toString());
      setShareNotice("URL copied to clipboard");
      if (shareTimerRef.current) {
        window.clearTimeout(shareTimerRef.current);
      }
      shareTimerRef.current = window.setTimeout(() => {
        setShareNotice(null);
      }, 2000);
    } catch {
      setShareNotice("Failed to copy URL");
      if (shareTimerRef.current) {
        window.clearTimeout(shareTimerRef.current);
      }
      shareTimerRef.current = window.setTimeout(() => {
        setShareNotice(null);
      }, 2000);
    }
  };

  const reportSelectedTestIssue = () => {
    if (!selected) return;
    const testUrl = `http://localhost:5555/?test=${encodeURIComponent(selected.name)}`;
    const title = `[Test] ${selected.name}`;
    const lines = [
      "## Test Details",
      `- Name: \`${selected.name}\``,
      `- URL: ${testUrl}`,
      `- Enabled: ${selected.enabled ? "true" : "false"}`,
      `- Write required: ${selected.writeRequired ? "true" : "false"}`,
      `- Tags: ${(selected.tags ?? []).join(", ") || "(none)"}`,
      "",
      "## Query",
      "```cypher",
      selected.query ?? "",
      "```"
    ];
    if (selectedResult) {
      lines.push(
        "",
        "## Last Run",
        `- Plan matched: ${selectedResult.planMatched ? "true" : "false"}`,
        `- Result matched: ${selectedResult.resultMatched ? "true" : "false"}`,
        `- Result JSON matched: ${selectedResult.resultJsonMatched ? "true" : "false"}`,
        typeof selectedResult.timeUs === "number"
          ? `- Time: ${selectedResult.timeUs} μs`
          : "- Time: (not available)"
      );
    }
    const params = new URLSearchParams({
      title,
      body: lines.join("\n")
    });
    const issueUrl = `https://github.com/turing-db/turingdb/issues/new?${params.toString()}`;
    window.open(issueUrl, "_blank", "noopener,noreferrer");
  };

  const selectedResult = selected ? results[selected.name] : undefined;
  const selectedRemoteResult = selected ? remoteResults[selected.name] : undefined;
  const selectedV3Result = selected ? v3Results[selected.name] : undefined;
  const getTestRunStatus = React.useCallback(
    (test: TestMeta) => {
      if (!test.enabled) return "disabled" as const;
      const localResult = results[test.name];
      if (!localResult) return "pending" as const;
      if (!isLocalPass(localResult)) return "fail" as const;
      const remoteResult = remoteResults[test.name];
      if (remoteResult && !isRemotePass(remoteResult)) return "fail" as const;
      const v3Result = v3Results[test.name];
      if (v3Result && !isV3Pass(v3Result)) return "fail" as const;
      return "pass" as const;
    },
    [isLocalPass, isRemotePass, isV3Pass, remoteResults, results, v3Results]
  );
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
      const status = getTestRunStatus(test);
      if (status === "disabled") return showDisabled;
      if (status === "pending") return showNotRun;
      return status === "pass" ? showPassing : showFailing;
    });
  }, [filteredTests, getTestRunStatus, tagFilters, showDisabled, showFailing, showNotRun, showPassing]);

  const stats = React.useMemo(() => {
    let passed = 0;
    let failed = 0;
    let disabled = 0;
    let notRun = 0;
    for (const test of tests) {
      const status = getTestRunStatus(test);
      switch (status) {
      case "disabled":
        disabled += 1;
        break;
      case "pending":
        notRun += 1;
        break;
      case "pass":
        passed += 1;
        break;
      case "fail":
        failed += 1;
        break;
      }
    }
    return { passed, failed, disabled, notRun };
  }, [getTestRunStatus, tests]);

  const showFailToast = React.useCallback((message: string) => {
    if (!message) return;
    setFailNotice(message);
    if (failTimerRef.current) {
      window.clearTimeout(failTimerRef.current);
    }
    failTimerRef.current = window.setTimeout(() => {
      setFailNotice(null);
    }, 2500);
  }, []);

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
    const source = expectedPlan?.trim();
    if (!source || (!source.startsWith("flowchart") && !source.startsWith("graph"))) {
      setPlanSvgExpected(null);
      return;
    }
    let cancelled = false;
    const id = `plan-expected-${renderSeq.current++}`;
    mermaid
      .render(id, source)
      .then((res) => {
        if (!cancelled) setPlanSvgExpected(res.svg);
      })
      .catch(() => {
        if (!cancelled) setPlanSvgExpected(null);
      });
    return () => {
      cancelled = true;
    };
  }, [expectedPlan]);

  React.useEffect(() => {
    const source = mainPlan?.trim();
    if (!source || (!source.startsWith("flowchart") && !source.startsWith("graph"))) {
      setPlanSvgMain(null);
      return;
    }
    let cancelled = false;
    const id = `plan-main-${renderSeq.current++}`;
    mermaid
      .render(id, source)
      .then((res) => {
        if (!cancelled) setPlanSvgMain(res.svg);
      })
      .catch(() => {
        if (!cancelled) setPlanSvgMain(null);
      });
    return () => {
      cancelled = true;
    };
  }, [mainPlan]);

  React.useEffect(() => {
    const parseCsv = (output: string) => {
      const trimmed = output.trim();
      if (!trimmed) return null;
      if (/error/i.test(trimmed) || trimmed.startsWith("ANALYZE") || trimmed.startsWith("EXEC")) {
        return null;
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
      return rows;
    };

    setParsedResult(parseCsv(selectedResult?.resultOutput ?? ""));
    setParsedRemoteResult(parseCsv(selectedRemoteResult?.resultOutput ?? ""));
    setParsedExpectedResult(parseCsv(expectedResult));
    setParsedMainResult(parseCsv(mainResult));
    setParsedV3Result(parseCsv(selectedV3Result?.resultV3Output ?? ""));
  }, [selectedRemoteResult?.resultOutput, selectedResult?.resultOutput, selectedV3Result?.resultV3Output, expectedResult, mainResult]);

  const renderTableResult = React.useCallback((rows: string[][]) => (
    <div className="mt-3 max-h-[48rem] overflow-auto rounded-xl border border-white/5 bg-paper">
      <table className="w-full border-collapse text-left text-xs text-ink">
        <thead className="sticky top-0 bg-steel/60 text-ink/80">
          <tr>
            {rows[0].map((cell, idx) => (
              <th key={idx} className="border-b border-white/5 px-3 py-2 font-semibold">
                {cell}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(1).map((row, rowIdx) => (
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
  ), []);

  const renderTextResult = React.useCallback((value?: string, heightClass = "max-h-[48rem]") => (
    <pre className={`mt-3 overflow-auto whitespace-pre-wrap rounded-xl bg-paper p-3 text-xs font-mono text-ink ${heightClass}`}>
      {value || "(empty)"}
    </pre>
  ), []);

  const renderCsvOrText = React.useCallback(
    (rows: string[][] | null, value?: string, heightClass = "max-h-[48rem]") =>
      rows && rows.length > 0 ? renderTableResult(rows) : renderTextResult(value, heightClass),
    [renderTableResult, renderTextResult]
  );

  const renderJsonOutput = React.useCallback((raw?: string) => {
    if (!raw) {
      return renderTextResult(undefined, "max-h-[36rem]");
    }
    try {
      const parsed = JSON.parse(raw);
      return (
        <div className="mt-3 max-h-[36rem] overflow-auto rounded-xl bg-paper p-3 text-xs font-mono">
          <JsonView data={parsed} style={darkStyles} />
        </div>
      );
    } catch {
      return renderTextResult(raw, "max-h-[36rem]");
    }
  }, [renderTextResult]);

  return (
    <SidebarProvider ref={sidebarRef} defaultOpen sidebarWidth={sidebarWidth}>
      <Sidebar>
        <SidebarHeader className="gap-3 border-b border-sidebar-border px-4 py-4">
          <div className="flex items-center justify-between">
            <h2 className="text-base font-semibold">Available Tests</h2>
            <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.2em]">
              <span className="flex items-center gap-1 rounded-full border border-emerald-400/30 bg-emerald-500/10 px-2 py-1 text-emerald-200">
                <CheckCircle2 className="h-3 w-3" />
                {stats.passed}
              </span>
              <span
                className={`flex items-center gap-1 rounded-full px-2 py-1 text-red-200 ${
                  stats.failed > 0
                    ? "border-2 border-red-400/80 bg-red-500/25 shadow-[0_0_0_1px_rgba(239,68,68,0.25)]"
                    : "border border-red-400/30 bg-red-500/10"
                }`}
              >
                <AlertTriangle className="h-3 w-3" />
                {stats.failed}
              </span>
              <span className="flex items-center gap-1 rounded-full border border-amber-400/30 bg-amber-500/10 px-2 py-1 text-amber-200">
                <Ban className="h-3 w-3" />
                {stats.disabled}
              </span>
              <span className="flex items-center gap-1 rounded-full border border-slate-400/30 bg-white/5 px-2 py-1 text-slate-200">
                <Clock3 className="h-3 w-3" />
                {stats.notRun}
              </span>
            </div>
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
                  const remoteResult = remoteResults[test.name];
                  const v3Result = v3Results[test.name];
                  const status = getTestRunStatus(test);
                  const isPending = status === "pending";
                  const isDisabled = status === "disabled";
                  const isPass = status === "pass";
                  const remoteFailed = !!remoteResult && !isRemotePass(remoteResult);
                  const v3Failed = !!v3Result && !isV3Pass(v3Result);
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
                  const timingLabel =
                    isPass && typeof testResult?.timeUs === "number"
                      ? `${testResult.timeUs} μs`
                      : null;
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
                          {test.isNew ? (
                            <FilePlus className="ml-2 h-3 w-3 text-emerald-300" />
                          ) : test.mainVersion || test.changed ? (
                            <GitCompareArrows className="ml-2 h-3 w-3 text-amber-300" />
                          ) : null}
                          <span
                            className={`ml-auto rounded-full px-2 py-0.5 text-[10px] tracking-[0.16em] ${badgeClass}`}
                          >
                          {isDisabled
                            ? "disabled"
                            : isPending
                              ? "pending"
                              : isPass
                                ? timingLabel ?? "pass"
                                : remoteFailed
                                  ? "remote fail"
                                  : v3Failed
                                    ? "ir fail"
                                    : "fail"}
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
                <PlayCircle />
                Run All
              </Button>
              <Dialog open={newTestOpen} onOpenChange={setNewTestOpen}>
                <DialogTrigger asChild>
                  <Button variant="ghost" disabled={loading}>
                    <Plus />
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
              <Button variant="ghost" onClick={duplicateSelectedTest} disabled={!selected || loading}>
                <Copy />
                Duplicate
              </Button>
              <Button variant="ghost" onClick={shareSelected} disabled={!selected}>
                <Share2 />
                Share
              </Button>
              <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
                <DialogTrigger asChild>
                  <Button variant="ghost" disabled={!selected || loading}>
                    <Trash2 />
                    Delete
                  </Button>
                </DialogTrigger>
                <DialogContent className="surface border-white/10">
                  <DialogHeader>
                    <DialogTitle>Delete test</DialogTitle>
                    <DialogDescription>
                      This will permanently delete the selected test.
                    </DialogDescription>
                  </DialogHeader>
                  <DialogFooter>
                    <Button variant="ghost" onClick={() => setDeleteOpen(false)}>
                      Cancel
                    </Button>
                    <Button variant="accent" onClick={deleteTest} disabled={loading}>
                      Delete
                    </Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
              <Button variant="accent" onClick={() => selected && runTest(selected.name)} disabled={!selected || loading}>
                <Play />
                Run Selected
              </Button>
            </div>
          </header>
          {shareNotice && (
            <div className="fixed right-6 top-6 z-50 animate-slide-in rounded-full border border-white/10 bg-steel/80 px-4 py-2 text-xs uppercase tracking-[0.2em] text-ink shadow-lg">
              {shareNotice}
            </div>
          )}
          {failNotice && (
            <div className="fixed right-6 top-16 z-50 animate-slide-in rounded-full border border-red-400/40 bg-red-500/15 px-4 py-2 text-xs uppercase tracking-[0.2em] text-red-100 shadow-lg">
              {failNotice}
            </div>
          )}
          {duplicateOpen && (
            <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30">
              <div className="surface rounded-2xl border border-white/10 px-6 py-4 text-sm text-ink">
                Duplicating test...
              </div>
            </div>
          )}

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

          {!selected && tests.length === 0 && (
            <div className="mt-6 flex flex-col items-center justify-center gap-3 rounded-2xl border border-white/10 bg-steel/40 p-10 text-center">
              <AlertTriangle className="h-8 w-8 text-amber-400/80" />
              <p className="text-sm text-ink/80">
                {error
                  ? "Could not load the test list. Check that the backend server and CLI binary are available."
                  : "No tests available."}
              </p>
              <Button variant="ghost" size="sm" onClick={() => { setError(null); loadTests(); }}>
                Retry
              </Button>
            </div>
          )}

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
              <Button
                variant="ghost"
                size="sm"
                onClick={reportSelectedTestIssue}
                disabled={!selected}
                className="mt-3"
              >
                <Bug />
                Report Issue
              </Button>
              {!selected.enabled && (
                <div className="mt-3">
                  <label className="text-[10px] uppercase tracking-[0.2em] text-ink/60">
                    Disabled reason
                  </label>
                  <div className="mt-2 flex items-center gap-2">
                    <input
                      value={disabledReasonDraft}
                      onChange={(event) => setDisabledReasonDraft(event.target.value)}
                      placeholder="Why is this test disabled?"
                      className="flex-1 rounded-xl border border-white/10 bg-paper px-3 py-2 text-xs text-ink focus:border-accent/60 focus:outline-none"
                    />
                    <Button variant="ghost" size="sm" onClick={updateDisabledReason} disabled={loading}>
                      Update
                    </Button>
                  </div>
                </div>
              )}
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

          {error && selected && (
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
                    <div className="flex items-center rounded-full border border-white/10 bg-paper/60 p-1 text-[10px] uppercase tracking-[0.18em] text-ink/70">
                      <button
                        className={`rounded-full px-2 py-1 ${resultTab === "actual" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setResultTab("actual")}
                      >
                        Actual
                      </button>
                      <button
                        className={`rounded-full px-2 py-1 ${resultTab === "expected" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setResultTab("expected")}
                      >
                        Expected
                      </button>
                      <button
                        className={`rounded-full px-2 py-1 ${resultTab === "main" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setResultTab("main")}
                        disabled={!selected?.changed}
                      >
                        Main
                      </button>
                    </div>
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
                {resultTab === "actual"
                  ? renderCsvOrText(parsedResult, selectedResult.resultOutput)
                  : resultTab === "expected"
                    ? renderCsvOrText(parsedExpectedResult, expectedResult)
                    : renderCsvOrText(parsedMainResult, mainResult)}
              </div>
              <div className="rounded-2xl border border-white/10 bg-steel/40 p-4">
                <div className="flex items-center justify-between">
                  <p className="text-xs uppercase tracking-[0.2em] text-ink/60">Plan Output</p>
                  <div className="flex items-center gap-2">
                    <div className="flex items-center rounded-full border border-white/10 bg-paper/60 p-1 text-[10px] uppercase tracking-[0.18em] text-ink/70">
                      <button
                        className={`rounded-full px-2 py-1 ${planTab === "actual" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setPlanTab("actual")}
                      >
                        Actual
                      </button>
                      <button
                        className={`rounded-full px-2 py-1 ${planTab === "expected" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setPlanTab("expected")}
                      >
                        Expected
                      </button>
                      <button
                        className={`rounded-full px-2 py-1 ${planTab === "main" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setPlanTab("main")}
                        disabled={!selected?.changed}
                      >
                        Main
                      </button>
                    </div>
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
                {planTab === "actual" ? (
                  planSvg ? (
                    <div
                      className="mt-3 max-h-[36rem] overflow-auto rounded-xl bg-paper p-3"
                      dangerouslySetInnerHTML={{ __html: planSvg }}
                    />
                  ) : (
                    <pre className="mt-3 max-h-[36rem] overflow-auto whitespace-pre-wrap rounded-xl bg-paper p-3 text-xs font-mono text-ink">
{selectedResult.planOutput || "(empty)"}
                    </pre>
                  )
                ) : planTab === "expected" ? (
                  planSvgExpected ? (
                    <div
                      className="mt-3 max-h-[36rem] overflow-auto rounded-xl bg-paper p-3"
                      dangerouslySetInnerHTML={{ __html: planSvgExpected }}
                    />
                  ) : (
                    <pre className="mt-3 max-h-[36rem] overflow-auto whitespace-pre-wrap rounded-xl bg-paper p-3 text-xs font-mono text-ink">
{expectedPlan || "(empty)"}
                    </pre>
                  )
                ) : planSvgMain ? (
                  <div
                    className="mt-3 max-h-[36rem] overflow-auto rounded-xl bg-paper p-3"
                    dangerouslySetInnerHTML={{ __html: planSvgMain }}
                  />
                ) : (
                  <pre className="mt-3 max-h-[36rem] overflow-auto whitespace-pre-wrap rounded-xl bg-paper p-3 text-xs font-mono text-ink">
{mainPlan || "(empty)"}
                  </pre>
                )}
              </div>
              <div className="rounded-2xl border border-white/10 bg-steel/40 p-4">
                <div className="flex items-center justify-between">
                  <p className="text-xs uppercase tracking-[0.2em] text-ink/60">JSON Result Output</p>
                  <div className="flex items-center gap-2">
                    <div className="flex items-center rounded-full border border-white/10 bg-paper/60 p-1 text-[10px] uppercase tracking-[0.18em] text-ink/70">
                      <button
                        className={`rounded-full px-2 py-1 ${jsonTab === "actual" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setJsonTab("actual")}
                      >
                        Actual
                      </button>
                      <button
                        className={`rounded-full px-2 py-1 ${jsonTab === "expected" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setJsonTab("expected")}
                      >
                        Expected
                      </button>
                      <button
                        className={`rounded-full px-2 py-1 ${jsonTab === "main" ? "bg-white/10 text-ink" : ""}`}
                        onClick={() => setJsonTab("main")}
                        disabled={!selected?.changed}
                      >
                        Main
                      </button>
                    </div>
                    {selectedResult.resultJsonValid != null && (
                      <span
                        className={`rounded-full px-2 py-1 text-[10px] uppercase tracking-[0.18em] ${
                          selectedResult.resultJsonValid
                            ? "bg-moss/15 text-moss"
                            : "bg-accent/15 text-accent"
                        }`}
                      >
                        {selectedResult.resultJsonValid ? "valid" : "invalid"}
                      </span>
                    )}
                    {selectedResult.resultJsonMatched != null && (
                      <span
                        className={`rounded-full px-2 py-1 text-[10px] uppercase tracking-[0.18em] ${
                          selectedResult.resultJsonMatched
                            ? "bg-moss/15 text-moss"
                            : "bg-accent/15 text-accent"
                        }`}
                      >
                        {selectedResult.resultJsonMatched ? "match" : "mismatch"}
                      </span>
                    )}
                    {selectedResult.resultJsonMatched === false && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setConfirmTarget("resultJson")}
                        disabled={loading}
                      >
                        Accept
                      </Button>
                    )}
                  </div>
                </div>
                {renderJsonOutput(
                  jsonTab === "actual"
                    ? selectedResult.resultJsonOutput
                    : jsonTab === "expected"
                      ? expectedResultJson
                      : mainResultJson
                )}
              </div>
            </div>
          )}

          {selected && (
            <div className="mt-6 grid gap-4">
              <div className="rounded-2xl border border-sky-400/20 bg-sky-500/5 p-4">
                <div>
                  <p className="text-xs uppercase tracking-[0.2em] text-sky-200/80">Remote Query Output</p>
                  <p className="mt-1 text-sm text-ink/70">
                    <code>Run All</code> also runs this test through the binary protocol client/server path. It compares the same
                    plain-text result corpus as the local suite.
                  </p>
                </div>
              </div>

              {selectedRemoteResult ? (
                <>
                  <div className="rounded-2xl border border-white/10 bg-steel/40 p-4">
                    <div className="flex items-center justify-between">
                      <p className="text-xs uppercase tracking-[0.2em] text-ink/60">Remote Result Output</p>
                      <div className="flex items-center gap-2">
                        <div className="flex items-center rounded-full border border-white/10 bg-paper/60 p-1 text-[10px] uppercase tracking-[0.18em] text-ink/70">
                          <button
                            className={`rounded-full px-2 py-1 ${remoteResultTab === "actual" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setRemoteResultTab("actual")}
                          >
                            Actual
                          </button>
                          <button
                            className={`rounded-full px-2 py-1 ${remoteResultTab === "expected" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setRemoteResultTab("expected")}
                          >
                            Expected
                          </button>
                          <button
                            className={`rounded-full px-2 py-1 ${remoteResultTab === "main" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setRemoteResultTab("main")}
                            disabled={!selected?.changed}
                          >
                            Main
                          </button>
                        </div>
                        <span
                          className={`rounded-full px-2 py-1 text-[10px] uppercase tracking-[0.18em] ${
                            selectedRemoteResult.resultMatched
                              ? "bg-moss/15 text-moss"
                              : "bg-accent/15 text-accent"
                          }`}
                        >
                          {selectedRemoteResult.resultMatched ? "match" : "mismatch"}
                        </span>
                        {typeof selectedRemoteResult.timeUs === "number" && (
                          <span className="rounded-full bg-white/5 px-2 py-1 text-[10px] uppercase tracking-[0.18em] text-ink/70">
                            {selectedRemoteResult.timeUs} us
                          </span>
                        )}
                      </div>
                    </div>
                    {remoteResultTab === "actual"
                      ? renderCsvOrText(parsedRemoteResult, selectedRemoteResult.resultOutput)
                      : remoteResultTab === "expected"
                        ? renderCsvOrText(parsedExpectedResult, expectedResult)
                        : renderCsvOrText(parsedMainResult, mainResult)}
                  </div>
                </>
              ) : (
                <div className="rounded-2xl border border-white/10 bg-steel/40 p-4 text-sm text-ink/70">
                  {selected?.remoteEnabled === false
                    ? `Remote run disabled${selected.remoteDisabledReason ? `: ${selected.remoteDisabledReason}` : "."}`
                    : <>Use <code>Run All</code> to populate remote output for this test.</>}
                </div>
              )}
            </div>
          )}

          {selected && (
            <div className="mt-6 grid gap-4">
              <div className="rounded-2xl border border-fuchsia-400/20 bg-fuchsia-500/5 p-4">
                <div>
                  <p className="text-xs uppercase tracking-[0.2em] text-fuchsia-200/80">V3 (MLIR) Query Output</p>
                  <p className="mt-1 text-sm text-ink/70">
                    <code>Run All</code> also runs this test through the <code>QueryInterpreterV3</code> MLIR path. Its result is
                    compared against the same <code>expect.result</code> as the local suite; the emitted DB MLIR program is compared
                    against <code>expect.mlir</code>.
                  </p>
                </div>
              </div>

              {selectedV3Result ? (
                <>
                  <div className="rounded-2xl border border-white/10 bg-steel/40 p-4">
                    <div className="flex items-center justify-between">
                      <p className="text-xs uppercase tracking-[0.2em] text-ink/60">V3 Result Output</p>
                      <div className="flex items-center gap-2">
                        <div className="flex items-center rounded-full border border-white/10 bg-paper/60 p-1 text-[10px] uppercase tracking-[0.18em] text-ink/70">
                          <button
                            className={`rounded-full px-2 py-1 ${resultV3Tab === "actual" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setResultV3Tab("actual")}
                          >
                            Actual
                          </button>
                          <button
                            className={`rounded-full px-2 py-1 ${resultV3Tab === "expected" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setResultV3Tab("expected")}
                          >
                            Expected
                          </button>
                          <button
                            className={`rounded-full px-2 py-1 ${resultV3Tab === "main" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setResultV3Tab("main")}
                            disabled={!selected?.changed}
                          >
                            Main
                          </button>
                        </div>
                        <span
                          className={`rounded-full px-2 py-1 text-[10px] uppercase tracking-[0.18em] ${
                            selectedV3Result.resultV3Matched
                              ? "bg-moss/15 text-moss"
                              : "bg-accent/15 text-accent"
                          }`}
                        >
                          {selectedV3Result.resultV3Matched ? "match" : "mismatch"}
                        </span>
                        {typeof selectedV3Result.timeUs === "number" && (
                          <span className="rounded-full bg-white/5 px-2 py-1 text-[10px] uppercase tracking-[0.18em] text-ink/70">
                            {selectedV3Result.timeUs} us
                          </span>
                        )}
                      </div>
                    </div>
                    {resultV3Tab === "actual"
                      ? renderCsvOrText(parsedV3Result, selectedV3Result.resultV3Output)
                      : resultV3Tab === "expected"
                        ? renderCsvOrText(parsedExpectedResult, expectedResult)
                        : renderCsvOrText(parsedMainResult, mainResult)}
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-steel/40 p-4">
                    <div className="flex items-center justify-between">
                      <p className="text-xs uppercase tracking-[0.2em] text-ink/60">MLIR Program</p>
                      <div className="flex items-center gap-2">
                        <div className="flex items-center rounded-full border border-white/10 bg-paper/60 p-1 text-[10px] uppercase tracking-[0.18em] text-ink/70">
                          <button
                            className={`rounded-full px-2 py-1 ${mlirTab === "actual" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setMlirTab("actual")}
                          >
                            Actual
                          </button>
                          <button
                            className={`rounded-full px-2 py-1 ${mlirTab === "expected" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setMlirTab("expected")}
                          >
                            Expected
                          </button>
                          <button
                            className={`rounded-full px-2 py-1 ${mlirTab === "main" ? "bg-white/10 text-ink" : ""}`}
                            onClick={() => setMlirTab("main")}
                            disabled={!selected?.changed}
                          >
                            Main
                          </button>
                        </div>
                        <span
                          className={`rounded-full px-2 py-1 text-[10px] uppercase tracking-[0.18em] ${
                            selectedV3Result.mlirMatched
                              ? "bg-moss/15 text-moss"
                              : "bg-accent/15 text-accent"
                          }`}
                        >
                          {selectedV3Result.mlirMatched ? "match" : "mismatch"}
                        </span>
                        {!selectedV3Result.mlirMatched && (
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => setConfirmTarget("mlir")}
                            disabled={loading}
                          >
                            Accept
                          </Button>
                        )}
                      </div>
                    </div>
                    {renderTextResult(
                      mlirTab === "actual"
                        ? selectedV3Result.mlirProgram
                        : mlirTab === "expected"
                          ? expectedMlir
                          : mainMlir
                    )}
                  </div>
                </>
              ) : (
                <div className="rounded-2xl border border-white/10 bg-steel/40 p-4 text-sm text-ink/70">
                  Use <code>Run All</code> to populate v3 output for this test.
                </div>
              )}
            </div>
          )}

          {(!selected || !selectedResult) && !loading && !error && (
            <div className="mt-6 rounded-2xl border border-white/10 bg-steel/40 p-4 text-sm text-ink/70">
              {selected
                ? "Run the local query path to populate local plan/result output."
                : "Connect the C++ runner API to populate plan/result output."}
            </div>
          )}
        </section>
          </main>
          {confirmTarget && (
            <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30">
              <div className="surface w-full max-w-md rounded-3xl p-6">
                <h3 className="text-lg font-semibold">Update test expectations?</h3>
              <p className="mt-2 text-sm text-ink/70">
                This will overwrite the expected {confirmTarget === "resultJson" ? "JSON result" : confirmTarget === "mlir" ? "MLIR program" : confirmTarget} in the JSON file for{" "}
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
