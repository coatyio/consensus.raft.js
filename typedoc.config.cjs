/*! Copyright 2023 Siemens AG

   Licensed under the Apache License, Version 2.0 (the "License"); you may not
   use this file except in compliance with the License. You may obtain a copy of
   the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
   License for the specific language governing permissions and limitations under
   the License.
*/

// Typedoc options (execute "npx typedoc -help")

module.exports = {
    // Can be used to prevent TypeDoc from cleaning the output directory specified with --out.
    cleanOutputDir: false,

    // Prevent externally resolved TypeScript files from being documented.
    excludeExternals: false,

    // Prevent private members from being included in the generated documentation.
    excludePrivate: true,

    // Ignores protected variables and methods
    excludeProtected: false,

    // Specifies the location to look for included documents.
    // Use [[include:FILENAME]] in comments.
    includes: "./",

    // Add the package version to the project name
    includeVersion: true,

    readme: "none",

    // Specifies the location the documentation should be written to.
    out: `docs/api/`,

    // Specifies the entry points to be documented by TypeDoc. TypeDoc will
    // examine the exports of these files and create documentation according
    // to the exports.
    entryPoints: [`src/index.ts`],

    // Sets the name for the default category which is used when only some
    // elements of the page are categorized. Defaults to 'Other'
    defaultCategory: "Uncategorized",

    // This flag categorizes reflections by group (within properties,
    // methods, etc). To allow methods and properties of the same category
    // to be grouped together, set this flag to false. Defaults to true.
    categorizeByGroup: false,

    // Array option which allows overriding the order categories display in.
    // A string of * indicates where categories that are not in the list
    // should appear.
    categoryOrder: [
        "Raft Controller",
        "Raft",
        "Raft Controller Errors",
        "Raft Interfaces",
        "Messages (one-to-one)",
        "Join Requests (one-to-many)",
        "*",
    ],
};
