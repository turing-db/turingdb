export interface GetDBStatusResponse {
  is_loaded: boolean;
  is_loading: boolean;
}

export const getDBStatus = async (db: string): Promise<GetDBStatusResponse> => {
  const res = fetch(`/api/get_db_status?db=${db}`, { method: "POST" }).then(
    (res) =>
      res.json().then((json) => {
        return json.data;
      }),
  );
  return res;
};

export const listAvailDBs = async (): Promise<string[]> => {
  return fetch("/api/list_avail_db", { method: "POST" }).then((res) =>
    res.json().then((json) => {
      return json.data.flat();
    }),
  );
};
