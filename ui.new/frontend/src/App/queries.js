import { useQuery as useNativeQuery } from '@tanstack/react-query';

export const useQuery = (key, fn, options = {}) =>
    useNativeQuery(key, fn, {
        immutableCheck: { warnAfter: 256 },
        serializableCheck: { warnAfter: 256 },
        refetchOnMount: false,
        refetchOnWindowFocus: false,
        staleTime: 100_000,
        ...options
    })

