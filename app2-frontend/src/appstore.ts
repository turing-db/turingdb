import { create } from "zustand";

export type PageType = "explore" | "viewer";
export type ThemeType = "dark" | "light";
export type DBType = {
  name: string;
  loaded: boolean;
  loading: boolean;
};

export type SetFuncType = (arg: {
  serverReady?: boolean;
  theme?: ThemeType;
  page?: PageType;
  db?: DBType;
}) => void;

const defaultStore = (set: SetFuncType) => ({
  serverReady: false,
  setServerReady: (v: boolean) => set({ serverReady: v}),

  theme: "dark" as ThemeType,
  setTheme: (v: ThemeType) => set({ theme: v }),

  page: "explore" as PageType,
  setPage: (v: PageType) => set({ page: v }),

  db: null as DBType | null,
  setDb: (v: DBType) => set({ db: v, serverReady: false }),
});

export type AppStore = ReturnType<typeof defaultStore>;

export const useAppStore = create<AppStore>(defaultStore);
