import globals from "globals";
import pluginJs from "@eslint/js";
import tseslint from "typescript-eslint";
import pluginReact from "eslint-plugin-react";
import pluginReactHooks from "eslint-plugin-react-hooks";
import { fixupPluginRules } from "@eslint/compat";

export default [ 
  { files: ["**/*.{js,mjs,cjs,ts,jsx,tsx}"]},
  {languageOptions: { globals: globals.browser }},
  pluginJs.configs.recommended,
  ...tseslint.configs.recommended,

  pluginReact.configs.flat.recommended,
  pluginReact.configs.flat["jsx-runtime"],

  { 
    plugins: {
      "react-hooks": fixupPluginRules(pluginReactHooks)
    },
    rules: pluginReactHooks.configs.recommended.rules
  },
];
