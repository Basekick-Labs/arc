//go:build linux

package sysmem

import (
	"testing"
	"testing/fstest"
)

// These cover limit(), cgroupV2Limit(), hostMemory() and readFileLimited() —
// the composition logic, which the parsing tests do not reach. They are
// linux-tagged because they exercise the linux reader, and CI runs on linux.
//
// Written because the first version of this change shipped without them: the
// plan promised fstest.MapFS coverage, the doc comment on rootFS claimed tests
// override it, and nothing did. Two real defects were in exactly this gap.

const meminfo16GiB = "MemTotal:       16777216 kB\nMemFree: 1234 kB\n"

func withFS(t *testing.T, files map[string]string) {
	t.Helper()
	m := fstest.MapFS{}
	for name, body := range files {
		m[name] = &fstest.MapFile{Data: []byte(body)}
	}
	prev := rootFS
	rootFS = m
	t.Cleanup(func() { rootFS = prev })
}

func TestLimit_Composition(t *testing.T) {
	const (
		twoGiBStr   = "2147483648\n"
		eightGiBStr = "8589934592\n"
		oneGiBStr   = "1073741824\n"
	)
	for _, c := range []struct {
		name       string
		files      map[string]string
		want       uint64
		wantSource Source
		wantOK     bool
	}{
		{
			name: "cgroup v2 limit at the namespace root",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": twoGiBStr,
				"proc/meminfo":             meminfo16GiB,
			},
			want: twoGiB, wantSource: SourceCgroupV2, wantOK: true,
		},
		{
			name: "cgroup v2 unlimited falls through to host memory",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": "max\n",
				"proc/meminfo":             meminfo16GiB,
			},
			want: 16 * oneGiB, wantSource: SourceMemInfo, wantOK: true,
		},
		{
			// --cgroupns=host: the real root has no memory interface files, so
			// only the path named in /proc/self/cgroup has the limit. Verified
			// against a live `docker run --cgroupns=host --memory=512m`.
			name: "cgroupns=host resolves via /proc/self/cgroup",
			files: map[string]string{
				"proc/self/cgroup":                       "0::/docker/abc123\n",
				"sys/fs/cgroup/docker/abc123/memory.max": twoGiBStr,
				"proc/meminfo":                           meminfo16GiB,
			},
			want: twoGiB, wantSource: SourceCgroupV2, wantOK: true,
		},
		{
			// A descendant cgroup can be tighter than the namespace root.
			// Preferring the root reported 8 GiB for a process actually held to
			// 1 GiB; the minimum is right under both namespace layouts.
			name: "tighter leaf wins over a looser root",
			files: map[string]string{
				"sys/fs/cgroup/memory.max":            eightGiBStr,
				"proc/self/cgroup":                    "0::/init.scope\n",
				"sys/fs/cgroup/init.scope/memory.max": oneGiBStr,
				"proc/meminfo":                        meminfo16GiB,
			},
			want: oneGiB, wantSource: SourceCgroupV2, wantOK: true,
		},
		{
			name: "memory.high below memory.max is the effective ceiling",
			files: map[string]string{
				"sys/fs/cgroup/memory.max":  twoGiBStr,
				"sys/fs/cgroup/memory.high": oneGiBStr,
				"proc/meminfo":              meminfo16GiB,
			},
			want: oneGiB, wantSource: SourceCgroupV2, wantOK: true,
		},
		{
			name: "cgroup v1 limit",
			files: map[string]string{
				"sys/fs/cgroup/memory/memory.limit_in_bytes": twoGiBStr,
				"proc/meminfo": meminfo16GiB,
			},
			want: twoGiB, wantSource: SourceCgroupV1, wantOK: true,
		},
		{
			name: "cgroup v1 unlimited sentinel falls through to host memory",
			files: map[string]string{
				"sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n",
				"proc/meminfo": meminfo16GiB,
			},
			want: 16 * oneGiB, wantSource: SourceMemInfo, wantOK: true,
		},
		{
			// THE REGRESSION. With no physical reading the clamp cannot fire, so
			// the v1 sentinel would be accepted as a ~9 exabyte limit — which
			// derives a 2184.5 PiB per-subprocess limit that DuckDB accepts. That
			// is #1026 reborn through the path meant to prevent it.
			name: "cgroup v1 sentinel with unreadable meminfo must report nothing",
			files: map[string]string{
				"sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n",
			},
			wantSource: SourceUnknown,
		},
		{
			name: "hybrid v1 and v2: v2 is preferred",
			files: map[string]string{
				"sys/fs/cgroup/memory.max":                   twoGiBStr,
				"sys/fs/cgroup/memory/memory.limit_in_bytes": eightGiBStr,
				"proc/meminfo":                               meminfo16GiB,
			},
			want: twoGiB, wantSource: SourceCgroupV2, wantOK: true,
		},
		{
			name:  "no cgroup at all falls back to host memory",
			files: map[string]string{"proc/meminfo": meminfo16GiB},
			want:  16 * oneGiB, wantSource: SourceMemInfo, wantOK: true,
		},
		{
			name:       "nothing readable at all",
			files:      map[string]string{},
			wantSource: SourceUnknown,
		},
		{
			// A cgroup limit above physical memory is not a limit.
			name: "cgroup limit above physical falls through",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": "34359738368\n", // 32 GiB on a 16 GiB box
				"proc/meminfo":             meminfo16GiB,
			},
			want: 16 * oneGiB, wantSource: SourceMemInfo, wantOK: true,
		},
		{
			// A truncated read must not become a tiny "physical memory" figure
			// that then discards a perfectly good cgroup limit.
			name: "malformed meminfo does not poison a real cgroup limit",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": twoGiBStr,
				"proc/meminfo":             "MemTotal: 167",
			},
			want: twoGiB, wantSource: SourceCgroupV2, wantOK: true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			withFS(t, c.files)
			got, source, ok := Limit()
			if ok != c.wantOK || got != c.want || source != c.wantSource {
				t.Fatalf("Limit() = (%d, %s, %v), want (%d, %s, %v)",
					got, source, ok, c.want, c.wantSource, c.wantOK)
			}
		})
	}
}

// Physical ignores every cgroup limit and reads the machine.
func TestPhysical(t *testing.T) {
	for _, c := range []struct {
		name   string
		files  map[string]string
		want   uint64
		wantOK bool
	}{
		{
			name: "cgroup limit does not change physical",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": "2147483648\n",
				"proc/meminfo":             meminfo16GiB,
			},
			want: 16 * oneGiB, wantOK: true,
		},
		{name: "no meminfo", files: map[string]string{"sys/fs/cgroup/memory.max": "2147483648\n"}},
		{name: "malformed meminfo", files: map[string]string{"proc/meminfo": "MemTotal: 167"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			withFS(t, c.files)
			got, source, ok := Physical()
			if ok != c.wantOK || got != c.want {
				t.Fatalf("Physical() = (%d, %s, %v), want (%d, %v)", got, source, ok, c.want, c.wantOK)
			}
			if ok && source != SourceMemInfo {
				t.Fatalf("source = %s, want %s", source, SourceMemInfo)
			}
		})
	}
}

// The telemetry collector's "which reports carry a real total_gb" query keys
// off usable_gb being non-null (#1412). That holds only while Limit falls back
// to host memory with no cgroup limit, so a readable machine always yields a
// usable figure too. If Limit ever starts answering "no limit" instead, this
// fails, and that query has to change with it.
func TestPhysicalImpliesLimit(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"meminfo only":       {"proc/meminfo": meminfo16GiB},
		"cgroup v2 max":      {"proc/meminfo": meminfo16GiB, "sys/fs/cgroup/memory.max": "max\n"},
		"cgroup v1 sentinel": {"proc/meminfo": meminfo16GiB, "sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n"},
	} {
		t.Run(name, func(t *testing.T) {
			withFS(t, files)
			if _, _, ok := Physical(); !ok {
				t.Fatal("Physical() not ok")
			}
			if _, _, ok := Limit(); !ok {
				t.Fatal("Physical() succeeded but Limit() did not")
			}
		})
	}
}
