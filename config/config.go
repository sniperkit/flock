//  Copyright (c) 2015 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	// token maps
	_ "github.com/wrble/flock/analysis/tokenmap"

	// fragment formatters
	_ "github.com/wrble/flock/search/highlight/format/ansi"
	_ "github.com/wrble/flock/search/highlight/format/html"

	// fragmenters
	_ "github.com/wrble/flock/search/highlight/fragmenter/simple"

	// highlighters
	_ "github.com/wrble/flock/search/highlight/highlighter/ansi"
	_ "github.com/wrble/flock/search/highlight/highlighter/html"
	_ "github.com/wrble/flock/search/highlight/highlighter/simple"

	// char filters
	_ "github.com/wrble/flock/analysis/char/html"
	_ "github.com/wrble/flock/analysis/char/regexp"
	_ "github.com/wrble/flock/analysis/char/zerowidthnonjoiner"

	// analyzers
	_ "github.com/wrble/flock/analysis/analyzer/custom"
	_ "github.com/wrble/flock/analysis/analyzer/keyword"
	_ "github.com/wrble/flock/analysis/analyzer/simple"
	_ "github.com/wrble/flock/analysis/analyzer/standard"
	_ "github.com/wrble/flock/analysis/analyzer/web"

	// token filters
	_ "github.com/wrble/flock/analysis/token/apostrophe"
	_ "github.com/wrble/flock/analysis/token/compound"
	_ "github.com/wrble/flock/analysis/token/edgengram"
	_ "github.com/wrble/flock/analysis/token/elision"
	_ "github.com/wrble/flock/analysis/token/keyword"
	_ "github.com/wrble/flock/analysis/token/length"
	_ "github.com/wrble/flock/analysis/token/lowercase"
	_ "github.com/wrble/flock/analysis/token/ngram"
	_ "github.com/wrble/flock/analysis/token/shingle"
	_ "github.com/wrble/flock/analysis/token/stop"
	_ "github.com/wrble/flock/analysis/token/truncate"
	_ "github.com/wrble/flock/analysis/token/unicodenorm"

	// tokenizers
	_ "github.com/wrble/flock/analysis/tokenizer/exception"
	_ "github.com/wrble/flock/analysis/tokenizer/regexp"
	_ "github.com/wrble/flock/analysis/tokenizer/single"
	_ "github.com/wrble/flock/analysis/tokenizer/unicode"
	_ "github.com/wrble/flock/analysis/tokenizer/web"
	_ "github.com/wrble/flock/analysis/tokenizer/whitespace"

	// date time parsers
	_ "github.com/wrble/flock/analysis/datetime/flexible"
	_ "github.com/wrble/flock/analysis/datetime/optional"

	// languages
	_ "github.com/wrble/flock/analysis/lang/ar"
	_ "github.com/wrble/flock/analysis/lang/bg"
	_ "github.com/wrble/flock/analysis/lang/ca"
	_ "github.com/wrble/flock/analysis/lang/cjk"
	_ "github.com/wrble/flock/analysis/lang/ckb"
	_ "github.com/wrble/flock/analysis/lang/cs"
	_ "github.com/wrble/flock/analysis/lang/el"
	_ "github.com/wrble/flock/analysis/lang/en"
	_ "github.com/wrble/flock/analysis/lang/eu"
	_ "github.com/wrble/flock/analysis/lang/fa"
	_ "github.com/wrble/flock/analysis/lang/fr"
	_ "github.com/wrble/flock/analysis/lang/ga"
	_ "github.com/wrble/flock/analysis/lang/gl"
	_ "github.com/wrble/flock/analysis/lang/hi"
	_ "github.com/wrble/flock/analysis/lang/hy"
	_ "github.com/wrble/flock/analysis/lang/id"
	_ "github.com/wrble/flock/analysis/lang/in"
	_ "github.com/wrble/flock/analysis/lang/it"
	_ "github.com/wrble/flock/analysis/lang/pt"

	// index types
	_ "github.com/wrble/flock/index/upsidedown"
)
