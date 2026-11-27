#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."

check_tree() {
    local package=$1
    shift
    local -a command=(cargo tree --locked -p "$package" --prefix none "$@")
    local tree
    printf '%q ' "${command[@]}"
    printf '\n'
    tree=$("${command[@]}")
    awk -v package="$package" '
        function reject(message) {
            print package, message > "/dev/stderr"
            failed = 1
        }
        $1 ~ /^(datafusion|orc-rust|paimon-mosaic-core|vortex)$/ {
            reject("has forbidden dependency " $1 " " $2)
        }
        $1 ~ /^datafusion-/ {
            datafusion = 1
            if (package == "paimon" || $2 != "v57.0.0-dev") {
                reject("has unexpected dependency " $1 " " $2)
            }
        }
        $1 ~ /^arrow($|-)/ || $1 == "parquet" {
            if ($2 != "v60.2.0") {
                reject("has unexpected dependency " $1 " " $2)
            }
        }
        $1 ~ /^(arrow($|-)|parquet$|datafusion-)/ && !seen[$1]++ {
            print $1, $2
        }
        END {
            if (!seen["arrow-array"] || !seen["parquet"]) {
                reject("is missing arrow-array or parquet")
            }
            if (package == "paimon-datafusion" && !datafusion) {
                reject("is missing DataFusion subcrates")
            }
            if (failed) {
                exit 1
            }
            print package, "dependency checks passed"
        }
    ' <<< "$tree"
}

check_tree paimon --manifest-path crates/paimon/Cargo.toml \
    --no-default-features --features storage-memory,storage-fs
check_tree paimon-datafusion "$@"
