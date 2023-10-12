import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App/components/App'
import '@fontsource/public-sans';
import 'normalize.css/normalize.css'
import '@blueprintjs/icons/lib/css/blueprint-icons.css'
import '@blueprintjs/core/lib/css/blueprint.css'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { Icons } from "@blueprintjs/icons";


const queryClient = new QueryClient();
const root = ReactDOM.createRoot(document.getElementById('root'));
Icons.setLoaderOptions({ loader: "all" });
await Icons.loadAll();

root.render(
    <QueryClientProvider client={queryClient}>
        <App />
    </QueryClientProvider>
);


