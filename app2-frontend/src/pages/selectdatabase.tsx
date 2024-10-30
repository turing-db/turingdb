import React from "react";

import { Icon, Spinner } from "@blueprintjs/core";

import TAppButton from "@/components/appbutton";
import { DBType, useAppStore } from "@/appstore";

const reqOpts = {
  method: "POST",
};

const fetchLoadDB = async (db: DBType) => {
  return fetch(`/api/load_db?db=${db.name}`, reqOpts);
};

export const TSelectDatabasePage: React.FC = () => {
  const db = useAppStore((state) => state.db);
  const setDb = useAppStore((state) => state.setDb);

  return (
    <div className="self-center flex-1 flex flex-col justify-center text-grey-600">
      <div className="flex flex-col align-center justify-center space-y-6">
        <Icon icon="database" size={50} className="self-center" />

        {db === null ? (
          <span className="font-bold">Select a database to start</span>
        ) : db.loading ? (
          <Spinner
            icon="database"
            size={50}
            intent={"success"}
            className="self-center"
          />
        ) : (
          <TAppButton
            onClick={() => {
              setDb({ ...db, loading: true, loaded: false });
              fetchLoadDB(db);
            }}
          >
            Load {db.name}
          </TAppButton>
        )}
      </div>
    </div>
  );
};
