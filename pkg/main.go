package exporter

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Sends a command to HAProxy UNIX socket and returns the response
func sendCommand(table string, socket string, storeType string, minRequestRate int, timeout time.Duration) (string, error) {
	switch {
	case storeType == "":
		return "", fmt.Errorf("storeType argument cannot be empty")
	case table == "":
		return "", fmt.Errorf("table argument cannot be empty")
	case socket == "":
		return "", fmt.Errorf("socket argument cannot be empty")
	case timeout < 0:
		return "", fmt.Errorf("timeout argument can't be negative")
	case minRequestRate < 0:
		return "", fmt.Errorf("minRequestRate argument can't be negative")
	}
	var d net.Dialer
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	d.LocalAddr = nil
	raddr := net.UnixAddr{Name: socket}
	conn, err := d.DialContext(ctx, "unix", raddr.String())
	if err != nil {
		return "", fmt.Errorf("Failed to connect to %s UNIX socket: %v", socket, err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return "", err
	}
	cmd := fmt.Sprintf("show table %s data.%s gt %d\n", table, storeType, minRequestRate)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return "", fmt.Errorf("Failed to send command to socket: %v", err)
	}

	buf := make([]byte, 1024)
	var data strings.Builder
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", fmt.Errorf("Error reading from socket: %v", err)
		}
		data.Write(buf[0:n])
	}
	r := strings.TrimSuffix(data.String(), "\n> ")
	r = strings.TrimSuffix(r, "\n")
	r = strings.TrimSpace(r)

	return r, nil
}

// Parses the response and returns a map of IP addresses to their request rates.
func parse(response string, expectedStoreDataType string) (map[netip.Addr]int, error) {

	requests := make(map[netip.Addr]int)
	if response == "" {
		return nil, fmt.Errorf("Response is empty or malformed")
	}

	lines := strings.Split(response, "\n")
	if len(lines) < 2 {
		return requests, nil
	}

	// Determine the stick table's data type.
	// Refer to http://docs.haproxy.org/dev/configuration.html#4.2-stick-table%20type for details.
	// Note: Stick tables can store multiple data types, which affect the response entries.
	// For example, with the following configuration:
	// backend table_requests_limiter_src_ip
	// stick-table type ip size 1m expire 60s store http_req_rate(60s),conn_cnt
	//
	// The response might include lines like:
	// 0x7fcf0c057200: key=127.0.0.1 use=0 exp=58330 shard=0 conn_cnt=3 http_req_rate(60000)=3
	//
	// The current regex only supports tables with a single increment rate data type.
	// Matches a line in this format:
	// 0x7fcf0c057200: key=127.0.0.1 use=0 exp=58330 shard=0 http_req_rate(60000)=3
	e := regexp.MustCompile(
		`^` +
			`\s*0x[[:alnum:]]+: ` + // Match the entry start with a hexadecimal address
			`key=(?P<ip>[0-9a-fA-F:.]+) ` + // Match and capture the IP address; 1st group
			`use=[[:digit:]]+ ` + // Match the use count
			`exp=[[:digit:]]+ ` + // Match the expiration time
			`shard=[[:digit:]]+` + // Match the shard value
			`(?: gpc\d=\d+)? ` + // Optionally match gpc field
			`(?P<storeType>[[:alnum:]_]+)` + // Match and capture the store type; 2nd group
			`\([[:digit:]]+\)=(?P<rate>[[:digit:]]+)$`, // Match and capture the rate; 3rd group
	)

	for i := 0; i < len(lines); i++ {
		m := e.FindStringSubmatch(lines[i])

		if len(m) == 4 {
			groups := make(map[string]string)
			for i, name := range e.SubexpNames() {
				if name != "" {
					groups[name] = m[i]
				}
			}

			storeType := groups["storeType"]
			if storeType != expectedStoreDataType {
				return nil, fmt.Errorf("Store type mismatch: expected '%s', but found '%s'", expectedStoreDataType, storeType)
			}
			ip, err := netip.ParseAddr(groups["ip"])
			if err != nil {
				return nil, fmt.Errorf("Failed to parse IP address: %v", err)
			}

			rate, err := strconv.Atoi(groups["rate"])
			if err != nil {
				return nil, fmt.Errorf("Failed to parse rate: %v", err)
			}
			// This is highly unlikely to occur. If it does, it indicates a bug in HAProxy.
			if _, ok := requests[ip]; ok {
				return nil, fmt.Errorf("Duplicate key detected: %s", ip)
			}

			requests[ip] = rate
		}
	}

	return requests, nil
}

// Check if the response is a stick-table of ip type and of expected name
func validateHeader(response string, expectedTableName string) error {
	lines := strings.Split(response, "\n")

	if len(lines) < 2 {
		return fmt.Errorf("Response is empty or malformed")
	}

	header := lines[0]
	// The first line must look like the one below, yes it starts with a #
	// # table: table_requests_limiter_src_ip, type: ip, size:1048576, used:2
	r := regexp.MustCompile(`^#\s+table:\s*(?P<tableName>[\w\-.]+)\s*,\s*type:\s*(?P<tableType>[[:alpha:]]+),`)
	m := r.FindStringSubmatch(header)

	if len(m) != 3 {
		return fmt.Errorf("Failed to parse table header, got '%s'", header)
	}

	tableName := m[1]
	tableType := m[2]
	if tableName != expectedTableName {
		return fmt.Errorf("Table name mismatch. Expected '%s', got '%s'", expectedTableName, tableName)
	}
	if tableType != "ip" {
		return fmt.Errorf("Unsupported table type '%s'. Only 'ip' type is supported", tableType)
	}

	return nil
}

// Run the exporter
func Run(table string, socket string, minimumRequestRate int, prometheusFile string) error {
	response, err := sendCommand(table, socket, "http_req_rate", minimumRequestRate, 1*time.Second)
	if err != nil {
		return err
	}
	if err := validateHeader(response, "table_requests_limiter_src_ip"); err != nil {
		return err
	}
	requests, err := parse(response, "http_req_rate")
	if err != nil {
		return err
	}

	metricsExporter := &StickTableExporter{
		metric: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "haproxy_stick_table",
				Help: "Tracks the 'http_req_rate' per client IP address as observed by custom stick-table in HAProxy",
			},
			[]string{"client_ip", "name", "type"},
		),
		stickData: make(map[netip.Addr]int),
		tableName: table,
	}

	metricsExporter.UpdateData(requests)
	if err := metricsExporter.WriteMetricsToFile(prometheusFile); err != nil {
		fmt.Printf("Error writing metrics to file: %v\n", err)
		os.Exit(1)
	}

	return nil
}
