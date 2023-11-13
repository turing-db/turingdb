import React from "react";

const SET_FILTERS = "SET_FILTERS";

export const filtersInitialState = () => ({
  showOnlyHomoSapiens: true,
  hidePublications: true,
  hideCompartments: true,
  hideSpecies: true,
  hideDatabaseReferences: true,
});

const useFiltersReducer = (state, action) => {
  switch (action.type) {
    case "CLEAR":
      return filtersInitialState();

    case SET_FILTERS: {
      return {
        ...state.filters,
        ...action.payload.filters,
      };
    }

    default:
      return state;
  }
};

const useFilters = () => {
  const [state, dispatch] = React.useReducer(
    useFiltersReducer,
    filtersInitialState()
  );

  const setFilters = React.useCallback(
    (filters) =>
      dispatch({
        type: SET_FILTERS,
        payload: { filters },
      }),
    []
  );

  return {
    filters: state,
    setFilters,
  };
};

export default useFilters;
