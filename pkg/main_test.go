package exporter

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func Test_sendCommand(t *testing.T) {
	t.Parallel()
	tmpDir, err := os.MkdirTemp("", "sendCommand-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	socket := filepath.Join(tmpDir, "haproxy.sock")

	tests := []struct {
		name              string
		table             string
		storeType         string
		minRequestRate    int
		timeout           time.Duration
		wantResult        string
		wantErr           bool
		connectionFailure bool
		expectedErr       string
	}{
		{
			name:           "valid input",
			table:          "table_requests_limiter_src_ip",
			storeType:      "http_req_rate",
			minRequestRate: 1,
			timeout:        1 * time.Second,
			expectedErr:    "",
			wantResult: "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=2321",
		},
		{
			name:              "connection failure",
			table:             "table_requests_limiter_src_ip",
			storeType:         "http_req_rate",
			minRequestRate:    1,
			timeout:           1 * time.Second,
			wantErr:           true,
			expectedErr:       "Failed to connect to",
			connectionFailure: true,
		},
		{
			name:        "connection timeout",
			table:       "table_requests_limiter_src_ip",
			storeType:   "http_req_rate",
			timeout:     0 * time.Nanosecond,
			wantErr:     true,
			expectedErr: "Failed to connect to",
		},
		{
			name:        "empty storeType",
			table:       "table_requests_limiter_src_ip",
			timeout:     1 * time.Second,
			wantErr:     true,
			expectedErr: "storeType argument",
		},
		{
			name:        "empty table",
			timeout:     1 * time.Second,
			storeType:   "http_req_rate",
			wantErr:     true,
			expectedErr: "table argument",
		},
		{
			name:        "negative timeout",
			timeout:     -1 * time.Second,
			storeType:   "http_req_rate",
			table:       "table_requests_limiter_src_ip",
			wantErr:     true,
			expectedErr: "timeout argument",
		},
		{
			name:           "negative minRequestRate",
			storeType:      "http_req_rate",
			table:          "table_requests_limiter_src_ip",
			minRequestRate: -1,
			wantErr:        true,
			expectedErr:    "minRequestRate argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up any existing socket
			os.Remove(socket)

			if tt.connectionFailure {
				// Create an empty socket file without a listener
				file, err := os.Create(socket)
				if err != nil {
					t.Fatalf("Failed to setup test %v", err)
				}
				file.Close()
			} else {
				// Setup the test socket
				listener, err := net.Listen("unix", socket)
				if err != nil {
					t.Fatalf("Failed to create Unix domain socket: %v", err)
				}
				defer listener.Close()
				// Start the mock server in a goroutine
				go func() {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					defer conn.Close()

					// Read the command
					buf := make([]byte, 1024)
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					expectedInput := fmt.Sprintf("show table %s data.%s gt %d\n", tt.table, tt.storeType, tt.minRequestRate)
					if string(buf[:n]) != expectedInput {
						t.Errorf("Expected input %q, got %q", expectedInput, string(buf[:n]))
					}

					if _, err := conn.Write([]byte(tt.wantResult)); err != nil {
						return
					}
				}()
			}

			got, err := sendCommand(tt.table, socket, tt.storeType, tt.minRequestRate, tt.timeout)
			// Check error cases
			if tt.wantErr != (err != nil) {
				t.Errorf("errored = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil && !strings.HasPrefix(err.Error(), tt.expectedErr) {
				t.Errorf("error message  --%v--, want something which starts with --%v--", err.Error(), tt.expectedErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.wantResult) {
					t.Errorf("sendCommand() returned %d lines, want %d lines", len(got), len(tt.wantResult))
					return
				}

				for i := range got {
					if got[i] != tt.wantResult[i] {
						t.Errorf("sendCommand() line %d = %v, want %v", i, got[i], tt.wantResult[i])
					}
				}
			}

		})
	}
}

func Test_parse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                  string
		input                 string
		wantErr               bool
		expectedErr           string
		expectedStoreDataType string
		expected              map[netip.Addr]int
	}{
		{
			name: "valid input",
			input: "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=2321",
			expectedStoreDataType: "http_req_rate",
			expected: func() map[netip.Addr]int {
				m := make(map[netip.Addr]int)
				addr1, _ := netip.ParseAddr("1.32.20.122")
				addr2, _ := netip.ParseAddr("1.39.115.67")
				m[addr1] = 1
				m[addr2] = 2321
				return m
			}(),
			wantErr:     false,
			expectedErr: "",
		},
		{
			name:                  "valid input without entries",
			input:                 "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597",
			expectedStoreDataType: "http_req_rate",
			expected:              map[netip.Addr]int{},
			wantErr:               false,
			expectedErr:           "",
		},
		{
			name: "invalid input with missing key", // we skip that entry and return valid response
			input: "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: use=0 exp=44496 shard=0 http_req_rate(60000)=2321",
			expectedStoreDataType: "http_req_rate",
			expected: func() map[netip.Addr]int {
				m := make(map[netip.Addr]int)
				addr1, _ := netip.ParseAddr("1.32.20.122")
				m[addr1] = 1
				return m
			}(),
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "invalid input with incorrect store type",
			input: "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 gpc,http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=11.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=2321\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 httpfoo_req_rate(60000)=2321",
			expectedStoreDataType: "http_req_rate",
			expected:              nil,
			wantErr:               true,
			expectedErr:           "Store type mismatch",
		},
		{
			name: "invalid input with incorrect IP address",
			input: "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=11.3 use=0 exp=44496 shard=0 http_req_rate(60000)=2321\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 httpfoo_req_rate(60000)=2321",
			expectedStoreDataType: "http_req_rate",
			expected:              nil,
			wantErr:               true,
			expectedErr:           "Failed to parse IP",
		},
		{
			name: "invalid input with incorrect rate",
			input: "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=-1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=as345esdf",
			expectedStoreDataType: "http_req_rate",
			expected: func() map[netip.Addr]int {
				m := make(map[netip.Addr]int)
				addr1, _ := netip.ParseAddr("1.32.20.122")
				m[addr1] = 1
				return m
			}(),
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "valid input with high rate",
			input: "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=100000000000000",
			expectedStoreDataType: "http_req_rate",
			expected: func() map[netip.Addr]int {
				m := make(map[netip.Addr]int)
				addr1, _ := netip.ParseAddr("1.32.20.122")
				addr2, _ := netip.ParseAddr("1.39.115.67")
				m[addr1] = 1
				m[addr2] = 100000000000000
				return m
			}(),
			wantErr:     false,
			expectedErr: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests, err := parse(tt.input, tt.expectedStoreDataType)
			// Check error cases
			if tt.wantErr != (err != nil) {
				t.Errorf("errored = %v, wantErr %v", err, tt.wantErr)
			}
			// If we don't expect an error compared the returned data
			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, requests); diff != "" {
					t.Error(diff)
				}
			}
			// If we expect an error, verify the error message
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error message = %v, got nil", tt.expectedErr)
				}
			}
		})
	}
}
func Test_validateHeader(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		input             string
		expectedTableName string
		wantErr           bool
		expectedErr       string
	}{
		{
			name: "valid input",
			input: "# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=2321",
			expectedTableName: "table_requests_limiter_src_ip",
			wantErr:           false,
			expectedErr:       "",
		},
		{
			name:              "empty input",
			input:             "",
			expectedTableName: "table_requests_limiter_src_ip",
			wantErr:           true,
			expectedErr:       "Response is empty",
		},
		{
			name: "invalid format with missing type",
			input: "# table: table_requests_limiter_src_ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=2321",
			expectedTableName: "table_requests_limiter_src_ip",
			wantErr:           true,
			expectedErr:       "Failed to parse",
		},
		{
			name: "valid input with wrong table name",
			input: "# table: wrong_table_name, type: ip, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=2321",
			expectedTableName: "table_requests_limiter_src_ip",
			wantErr:           true,
			expectedErr:       "Table",
		},
		{
			name: "valid input with wrong type",
			input: "# table: table_requests_limiter_src_ip, type: xfoop, size:1048576, used:11597\n" +
				"0x7f6d48298b70: key=1.32.20.122 use=0 exp=26834 shard=0 http_req_rate(60000)=1\n" +
				"0x55e0d8f5cc20: key=1.39.115.67 use=0 exp=44496 shard=0 http_req_rate(60000)=2321",
			expectedTableName: "table_requests_limiter_src_ip",
			wantErr:           true,
			expectedErr:       "Unsupported table type",
		},
		{
			name:              "empty input with newline character",
			input:             "\n",
			expectedTableName: "table_requests_limiter_src_ip",
			wantErr:           true,
			expectedErr:       "Failed to parse table header",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateHeader(tt.input, tt.expectedTableName)
			// Check error cases
			if tt.wantErr != (err != nil) {
				t.Errorf("validateHeader() errored = %v, wantErr %v", err, tt.wantErr)
			}

			// If we expect an error, verify the error message
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error message = %v, got nil", tt.expectedErr)
				}
				if !strings.HasPrefix(err.Error(), tt.expectedErr) {
					t.Errorf("error message  --%v--, want something which starts with --%v--", err.Error(), tt.expectedErr)
				}
			}
		})
	}
}
func Fuzz_validateHeader(f *testing.F) {
	testcases := []string{
		"# table: tasdsdsaer_src_ip, type: ip, size:1048576, used:11597\n",
		"# table: table_requests_limiter_src_ip, type: ip, size:1048576, used:11597\n",
	}
	for _, tc := range testcases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, in string) {
		err := validateHeader(in, "table_requests_limiter_src_ip")
		if err != nil {
			t.Skip("handled error")
		}
	})
}
