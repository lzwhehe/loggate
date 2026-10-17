// Package normalize maps ingested events to a subset of OCSF 1.3.0
// (DESIGN §6, scope reduced by §0.5): Authentication (3002) from sshd
// lines and JSON auth events, Base Event (0) for everything else.
// Network Activity (4001) is out of v1 scope.
//
// The output is a plain map so the masking engine can walk it and sinks can
// encode it as JSON. Numbers produced here are Go ints; values copied from
// the input keep their decoded type (json.Number for JSON numbers).
package normalize

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// OCSFVersion is the OCSF schema version whose subset is emitted.
const OCSFVersion = "1.3.0"

// ProductName is metadata.product.name.
const ProductName = "LogGate"

// OCSF identifiers used here.
const (
	ClassBase = 0
	ClassAuth = 3002

	CategoryUncategorized = 0
	CategoryIAM           = 3

	ActivityUnknown = 0
	ActivityLogon   = 1
	ActivityLogoff  = 2

	StatusUnknown = 0
	StatusSuccess = 1
	StatusFailure = 2

	SeverityUnknown       = 0
	SeverityInformational = 1
	SeverityLow           = 2

	AuthProtocolUnknown = 0
	AuthProtocolOther   = 99
)

// Event is a normalised event.
type Event struct {
	Fields     map[string]any
	ClassUID   int32
	ActivityID int32
	StatusID   int32
	SeverityID int32
	Time       int64 // unix nanoseconds
}

var (
	activityNames = map[int]string{ActivityUnknown: "Unknown", ActivityLogon: "Logon", ActivityLogoff: "Logoff"}
	statusNames   = map[int]string{StatusUnknown: "Unknown", StatusSuccess: "Success", StatusFailure: "Failure"}
	severityNames = map[int]string{SeverityUnknown: "Unknown", SeverityInformational: "Informational", SeverityLow: "Low"}
)

func metadata(logName string) map[string]any {
	return map[string]any{
		"version":  OCSFVersion,
		"product":  map[string]any{"name": ProductName, "vendor_name": ProductName},
		"log_name": logName,
	}
}

type auth struct {
	activity, status, severity int
	user                       string
	hasUser                    bool
	ip                         string
	port                       int
	protocol                   string
	detail                     string
	t                          time.Time
}

func (a auth) event(logName string, raw *string, unmapped map[string]any) Event {
	f := map[string]any{
		"class_uid": ClassAuth, "class_name": "Authentication",
		"category_uid": CategoryIAM, "category_name": "Identity & Access Management",
		"activity_id": a.activity, "activity_name": activityNames[a.activity],
		"type_uid": ClassAuth*100 + a.activity, "type_name": "Authentication: " + activityNames[a.activity],
		"status_id": a.status, "status": statusNames[a.status],
		"severity_id": a.severity, "severity": severityNames[a.severity],
		"time":     a.t.UnixMilli(),
		"metadata": metadata(logName),
	}
	if a.hasUser {
		f["user"] = map[string]any{"name": a.user}
	}
	if a.ip != "" {
		ep := map[string]any{"ip": a.ip}
		if a.port > 0 {
			ep["port"] = a.port
		}
		f["src_endpoint"] = ep
	}
	if a.protocol != "" {
		f["auth_protocol"] = a.protocol
		f["auth_protocol_id"] = AuthProtocolOther
	} else {
		f["auth_protocol_id"] = AuthProtocolUnknown
	}
	if a.detail != "" {
		f["status_detail"] = a.detail
	}
	if raw != nil {
		f["raw_data"] = *raw
	}
	if len(unmapped) > 0 {
		f["unmapped"] = unmapped
	}
	return Event{Fields: f, ClassUID: ClassAuth, ActivityID: int32(a.activity), StatusID: int32(a.status),
		SeverityID: int32(a.severity), Time: a.t.UnixNano()}
}

func base(logName string, t time.Time, raw *string, unmapped map[string]any) Event {
	f := map[string]any{
		"class_uid": ClassBase, "class_name": "Base Event",
		"category_uid": CategoryUncategorized, "category_name": "Uncategorized",
		"activity_id": ActivityUnknown, "activity_name": "Unknown",
		"type_uid": 0, "type_name": "Base Event: Unknown",
		"severity_id": SeverityUnknown, "severity": "Unknown",
		"time":     t.UnixMilli(),
		"metadata": metadata(logName),
	}
	if raw != nil {
		f["raw_data"] = *raw
	}
	if unmapped != nil {
		f["unmapped"] = unmapped
	}
	return Event{Fields: f, ClassUID: ClassBase, Time: t.UnixNano()}
}

var (
	// sshd authentication results ("Accepted password for root from
	// 192.0.2.1 port 22 ssh2", "Failed password for invalid user x ...").
	reSSHDAuth = regexp.MustCompile(`\b(Accepted|Failed) (\S+) for (invalid user )?(\S*) from (\S+) port (\d+)(?: (ssh\d?))?`)
	// "Connection closed by [authenticating|invalid user U ]IP port P [preauth]"
	reSSHDClosed = regexp.MustCompile(`\bConnection closed by (?:(?:authenticating|invalid) user (\S*) )?(\S+) port (\d+)( \[preauth\])?`)
	// "Disconnected from [user U ]IP port P"
	reSSHDDisc = regexp.MustCompile(`\bDisconnected from (?:(?:authenticating |invalid )?user (\S*) )?(\S+) port (\d+)( \[preauth\])?`)
	reSSHDTag  = regexp.MustCompile(`\bsshd(?:\[\d+\])?:`)
)

func port(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > 65535 {
		return 0
	}
	return n
}

// Text normalises one plain-text log line. sshd authentication lines
// become Authentication (3002); anything else is a Base Event carrying the
// line as raw_data. recv is the receive time (syslog timestamps carry no
// year and are not parsed).
func Text(line string, recv time.Time) Event {
	if reSSHDTag.MatchString(line) || strings.Contains(line, " port ") {
		if a, ok := sshd(line, recv); ok {
			return a.event("sshd", &line, nil)
		}
	}
	return base("text", recv, &line, nil)
}

func sshd(line string, recv time.Time) (auth, bool) {
	a := auth{t: recv, protocol: ""}
	if m := reSSHDAuth.FindStringSubmatch(line); m != nil {
		a.activity = ActivityLogon
		if m[1] == "Accepted" {
			a.status, a.severity = StatusSuccess, SeverityInformational
		} else {
			a.status, a.severity = StatusFailure, SeverityLow
		}
		a.protocol = m[2]
		if m[7] != "" {
			a.protocol = m[7] + "/" + m[2]
		}
		a.user, a.hasUser = m[4], true
		if m[3] != "" {
			a.detail = "invalid user"
		}
		a.ip, a.port = m[5], port(m[6])
		return a, true
	}
	if m := reSSHDClosed.FindStringSubmatch(line); m != nil {
		// Closed before authentication completed: a failed logon attempt.
		a.activity, a.status, a.severity = ActivityLogon, StatusFailure, SeverityLow
		a.detail = "connection closed before authentication"
		if m[4] == "" {
			a.activity, a.status, a.severity = ActivityLogoff, StatusSuccess, SeverityInformational
			a.detail = "connection closed"
		}
		a.user, a.hasUser = m[1], m[1] != ""
		a.ip, a.port = m[2], port(m[3])
		return a, true
	}
	if m := reSSHDDisc.FindStringSubmatch(line); m != nil {
		a.activity, a.status, a.severity = ActivityLogoff, StatusSuccess, SeverityInformational
		if m[4] != "" {
			a.activity, a.status, a.severity = ActivityLogon, StatusFailure, SeverityLow
			a.detail = "disconnected before authentication"
		}
		a.user, a.hasUser = m[1], m[1] != ""
		a.ip, a.port = m[2], port(m[3])
		return a, true
	}
	return a, false
}

// JSON normalises one decoded JSON object (numbers as json.Number). Objects
// with event.type == "auth" (nested {"event":{"type":"auth"}} or flat
// "event.type") become Authentication (3002) and the mapped keys are
// removed from unmapped; any other object is a Base Event whose unmapped
// holds the whole object unchanged. raw_data is not kept for JSON input
// (it would duplicate unmapped).
func JSON(obj map[string]any, recv time.Time) Event {
	if strings.EqualFold(str(peek(obj, "event", "type")), "auth") {
		return jsonAuth(obj, recv)
	}
	return base("json", recv, nil, obj)
}

// peek returns obj["a.b"] or obj["a"]["b"].
func peek(obj map[string]any, path ...string) any {
	if v, ok := obj[strings.Join(path, ".")]; ok {
		return v
	}
	cur := obj
	for i, p := range path {
		v, ok := cur[p]
		if !ok {
			return nil
		}
		if i == len(path)-1 {
			return v
		}
		if cur, ok = v.(map[string]any); !ok {
			return nil
		}
	}
	return nil
}

// take removes and returns the first present of the given dotted paths
// (flat key or nested), pruning emptied parent objects.
func take(obj map[string]any, paths ...string) (any, bool) {
	for _, p := range paths {
		if v, ok := obj[p]; ok {
			delete(obj, p)
			return v, true
		}
		parts := strings.Split(p, ".")
		if len(parts) == 1 {
			continue
		}
		parent, ok := obj[parts[0]].(map[string]any)
		if !ok {
			continue
		}
		if v, ok := take(parent, strings.Join(parts[1:], ".")); ok {
			if len(parent) == 0 {
				delete(obj, parts[0])
			}
			return v, true
		}
	}
	return nil, false
}

func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	}
	return ""
}

func jsonAuth(obj map[string]any, recv time.Time) Event {
	_, _ = take(obj, "event.type")
	a := auth{t: recv, activity: ActivityLogon, severity: SeverityUnknown}
	if v, ok := take(obj, "user.name", "username", "user"); ok {
		if s, isStr := v.(string); isStr {
			a.user, a.hasUser = s, true
		} else {
			obj["user"] = v // not a scalar name: keep it unmapped
		}
	}
	if v, ok := take(obj, "src_endpoint.ip", "source.ip", "src_ip", "client_ip"); ok {
		a.ip = str(v)
	}
	if v, ok := take(obj, "src_endpoint.port", "source.port", "src_port"); ok {
		a.port = port(str(v))
	}
	if v, ok := take(obj, "auth_protocol", "method"); ok {
		a.protocol = str(v)
	}
	if v, ok := take(obj, "event.action", "action"); ok {
		switch strings.ToLower(str(v)) {
		case "logout", "logoff", "signout", "sign_out":
			a.activity = ActivityLogoff
		}
	}
	if v, ok := take(obj, "event.outcome", "outcome", "result"); ok {
		switch strings.ToLower(str(v)) {
		case "success", "succeeded", "ok", "true", "accepted":
			a.status, a.severity = StatusSuccess, SeverityInformational
		case "failure", "failed", "fail", "false", "denied", "rejected":
			a.status, a.severity = StatusFailure, SeverityLow
		}
	}
	if v, ok := take(obj, "@timestamp", "timestamp", "time"); ok {
		if t, ok := parseTime(v); ok {
			a.t = t
		} else {
			obj["time"] = v // unparseable: keep it unmapped
		}
	}
	return a.event("json", nil, obj)
}

// parseTime accepts RFC 3339 strings and epoch milliseconds.
func parseTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		t, err := time.Parse(time.RFC3339Nano, x)
		return t, err == nil
	case json.Number:
		ms, err := x.Int64()
		if err != nil || ms < 0 {
			return time.Time{}, false
		}
		return time.UnixMilli(ms), true
	}
	return time.Time{}, false
}
