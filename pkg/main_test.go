package exporter

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_parse(t *testing.T) {
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
