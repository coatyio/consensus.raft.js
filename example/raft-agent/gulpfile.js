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

const fsextra = require("fs-extra");
const gulp = require("gulp");
const tsc = require("gulp-typescript");
const tslint = require("gulp-tslint");

/**
 * Clean distribution folder
 */
gulp.task("clean", () => {
    return fsextra.emptyDir("dist");
});

/**
* Copy Javascript files (not transpiled)
*/
gulp.task("copy:js", () => {
    return gulp
        .src(["src/**/*.js", "src/**/*.mjs"])
        .pipe(gulp.dest("dist"));
});

/**
 * Transpile TS into JS code, using TS compiler in local typescript npm package.
 * Remove all comments except copyright header comments, and do not generate
 * corresponding .d.ts files (see task "transpile:dts").
 */
 gulp.task("transpile:ts", () => {
    const tscConfig = require("./tsconfig.json");
    return gulp
        .src(["src/**/*.ts", "!src/index.typedoc.ts"])
        .pipe(tsc(Object.assign(tscConfig.compilerOptions, {
            removeComments: true,
            declaration: false,
        })))
        .pipe(gulp.dest("dist"));
});

/**
 * Only emit TS declaration files, using TS compiler in local typescript npm
 * package. The generated declaration files include all comments so that IDEs
 * can provide this information to developers.
 */
gulp.task("transpile:dts", () => {
    const tscConfig = require("./tsconfig.json");
    return gulp
        .src(["src/**/*.ts", "!src/index.typedoc.ts"])
        .pipe(tsc(Object.assign(tscConfig.compilerOptions, {
            removeComments: false,
            declaration: true,
        })))
        .dts
        .pipe(gulp.dest("dist"));
});

/**
 * Lint the application
 */
gulp.task("lint", () => {
    return gulp.src(["src/**/*.ts"])
        .pipe(tslint({
            configuration: "./tslint.json",
            formatter: "verbose",
        }))
        .pipe(tslint.report({
            emitError: false,
            summarizeFailureOutput: true
        }));
});

/**
 * Lint the application and fix lint errors
 */
gulp.task("lint:fix", () => {
    return gulp.src(["src/**/*.ts"])
        .pipe(tslint({
            configuration: "./tslint.json",
            formatter: "verbose",
            fix: true
        }))
        .pipe(tslint.report({
            emitError: false,
            summarizeFailureOutput: true
        }));
});;

gulp.task("build", gulp.series("clean", "copy:js", "transpile:ts", "transpile:dts", "lint"));

gulp.task("default", gulp.series("build"));
