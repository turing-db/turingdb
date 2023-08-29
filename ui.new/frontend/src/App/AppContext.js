import { useSelector } from 'react-redux';

export const useDbName = () => useSelector((state) => state.dbName);
export const useInspectedNode = () => useSelector((state) => state.inspectedNode);
export const useStringProperties = () => useSelector((state) => state.stringProperties);
