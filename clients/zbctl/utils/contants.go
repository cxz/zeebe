// Copyright © 2018 Camunda Services GmbH (info@camunda.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package utils

import (
	"fmt"
	"github.com/zeebe-io/zeebe/clients/go/commands"
	"github.com/zeebe-io/zeebe/clients/go/utils"
	"math"
	"time"
)

const (
	DefaultAddressHost = "127.0.0.1"
	DefaultAddressPort = 26500
	DefaultJobRetries  = utils.DefaultRetries
	DefaultJobWorker   = "zbctl"
	DefaultJobTimeout  = 5 * time.Minute
	LatestVersion      = commands.LatestVersion
	EmptyJsonObject    = "{}"
)

// process exit codes
const (
	ExitCodeGeneralError       = 1
	ExitCodeCommandNotFound    = 127
	ExitCodeConfigurationError = 78
	ExitCodeIOError            = 74
)

var (
	Version = "development"
	Commit  = "HEAD"
)

func VersionString() string {
	commit := Commit[0:int(math.Min(8, float64(len(Commit))))]
	return fmt.Sprintf("zbctl %s (commit: %s)", Version, commit)

}
