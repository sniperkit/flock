//  Copyright (c) 2014 Couchbase, Inc.
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

// Package en implements an analyzer with reasonable defaults for processing
// English text.
//
// It strips possessive suffixes ('s), transforms tokens to lower case,
// removes stopwords from a built-in list, and applies porter stemming.
//
// The built-in stopwords list is defined in EnglishStopWords.
package en

import (
	"github.com/wrble/flock/analysis"
	"github.com/wrble/flock/registry"

	"github.com/wrble/flock/analysis/token/lowercase"
	"github.com/wrble/flock/analysis/token/porter"
	"github.com/wrble/flock/analysis/tokenizer/unicode"
)

const AnalyzerName = "en"

func AnalyzerConstructor(config map[string]interface{}, cache *registry.Cache) (*analysis.Analyzer, error) {
	tokenizer, err := cache.TokenizerNamed(unicode.Name)
	if err != nil {
		return nil, err
	}
	possEnFilter, err := cache.TokenFilterNamed(PossessiveName)
	if err != nil {
		return nil, err
	}
	toLowerFilter, err := cache.TokenFilterNamed(lowercase.Name)
	if err != nil {
		return nil, err
	}
	stopEnFilter, err := cache.TokenFilterNamed(StopName)
	if err != nil {
		return nil, err
	}
	stemmerEnFilter, err := cache.TokenFilterNamed(porter.Name)
	if err != nil {
		return nil, err
	}
	rv := analysis.Analyzer{
		Tokenizer: tokenizer,
		TokenFilters: []analysis.TokenFilter{
			possEnFilter,
			toLowerFilter,
			stopEnFilter,
			stemmerEnFilter,
		},
	}
	return &rv, nil
}

func init() {
	registry.RegisterAnalyzer(AnalyzerName, AnalyzerConstructor)
}
