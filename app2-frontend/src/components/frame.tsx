import React from "react";

import { useAppStore } from "@/appstore";

import TAppButton from "@/components/appbutton";

import { TExplorePage } from "@/pages/explore";
import { TViewerPage } from "@/pages/viewer";
import { TSelectDatabasePage } from "@/pages/selectdatabase";
import { ItemPredicate, ItemRenderer, Select } from "@blueprintjs/select";
import { MenuItem } from "@blueprintjs/core";
import { getDBStatus, listAvailDBs } from "@/api/endpoints";

const renderer: ItemRenderer<string> = (db, { handleClick, handleFocus }) => {
  return (
    <MenuItem
      text={db}
      key={db}
      label={db}
      roleStructure="listoption"
      onClick={handleClick}
      onFocus={handleFocus}
    />
  );
};

const filter: ItemPredicate<string> = (query, db, _index, exactMatch) => {
  const normalizedEntry = db.toLowerCase();
  const normalizedQuery = query.toLowerCase();

  if (exactMatch) {
    return normalizedEntry === normalizedQuery;
  } else {
    return normalizedEntry.indexOf(normalizedQuery) >= 0;
  }
};

type DBSelectorProps = {
  setLoading: React.Dispatch<React.SetStateAction<boolean>>;
};

const DBSelector: React.FC<DBSelectorProps> = (props) => {
  const { setLoading } = props;
  const db = useAppStore((state) => state.db);
  const setDb = useAppStore((state) => state.setDb);
  const [dbs, setDbs] = React.useState<string[]>([]);

  React.useEffect(() => {
    listAvailDBs()
      .then((data) => {
        setDbs(data);
        setLoading(true);
      })
      .catch((err) => console.log(err));
  }, [setLoading]);

  return (
    <Select<string>
      items={dbs}
      itemRenderer={renderer}
      itemPredicate={filter}
      onItemSelect={(db) => {
        setDb({
          name: db,
          loading: false,
          loaded: false,
        });
        setLoading(true);
      }}
    >
      <TAppButton rightIcon="double-caret-vertical" highlight={db === null}>
        {db?.name || "Database"}
      </TAppButton>
    </Select>
  );
};

export const TFrame = () => {
  const page = useAppStore((state) => state.page);
  const db = useAppStore((state) => state.db);
  const setDb = useAppStore((state) => state.setDb);
  const [loading, setLoading] = React.useState(true);

  const fetchDbStatus = React.useCallback(
    async (db: string) => {
      return getDBStatus(db)
        .then((status) => {
          setDb({
            name: db,
            loading: status.is_loading,
            loaded: status.is_loaded,
          });
          setLoading(false);
        })
        .catch((err) => {
          console.log(err);
          setLoading(false);
        });
    },
    [setDb],
  );

  React.useEffect(() => {
    if (!loading) return;
    if (db === null) return;

    fetchDbStatus(db.name);
  }, [db, loading, fetchDbStatus]);

  React.useEffect(() => {
    if (db === null) return;
    if (!db.loading) return;

    const interval = setInterval(() => fetchDbStatus(db.name), 400);

    return () => clearInterval(interval);
  }, [db, fetchDbStatus]);

  const rightTitle = React.useMemo(
    () => db?.name || "No database selected",
    [db],
  );
  const leftTitle = React.useMemo(() => {
    switch (page) {
      case "explore":
        return "Exploring:";
      case "viewer":
        return "Viewing:";
    }
  }, [page]);

  const content = React.useMemo(() => {
    if (loading) {
      return;
    }

    if (db === null || !db.loaded) {
      return <TSelectDatabasePage />;
    }

    switch (page) {
      case "explore":
        return <TExplorePage />;
      case "viewer":
        return <TViewerPage />;
    }
  }, [db, page, loading]);

  return (
    <div className="flex-1 flex flex-col">
      <div className="border-b border-t border-grey-900 bg-grey-800 p-4 flex space-x-4 items-center justify-between">
        <div className="text-sm font-medium text-content-secondary font-sans flex space-x-7 items-center">
          <span>{leftTitle}</span>
          <span
            className={`border border-grey-500 rounded-md p-1 pl-2 pr-2 ml-4 text-content-secondary text-sm`}
          >
            {rightTitle}
          </span>
        </div>
        <DBSelector setLoading={setLoading} />
      </div>
      {content}
    </div>
  );
};
