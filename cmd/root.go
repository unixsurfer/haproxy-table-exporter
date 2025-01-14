package cmd

import (
	"fmt"
	exporter "haproxy-table-exporter/pkg"
	"io/fs"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var (
	socket             string
	prometheusFile     string
	stickTable         string
	minimumRequestRate int
	rootCmd            = &cobra.Command{
		Use:   "haproxy-table-exporter",
		Short: "A Prometheus textfile exporter for querying and exporting metrics from a specific stick-table in HAProxy",
		Long: `
A Prometheus exporter for querying HAProxy stick-tables and generating metrics.
It sends the "show table <stick-table-name>" command to HAProxy via a UNIX socket
and creates the metric haproxy_client_request_rate with client IPs as labels.

This tool supports only IP-type stick-tables with the http_req_rate data store.
It is intended to run as a cron job and requires write access to the UNIX socket
and the metrics directory.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			f, err := os.Stat(socket)
			if os.IsNotExist(err) {
				return err
			}
			if f.Mode().Type() != fs.ModeSocket {
				return fmt.Errorf("%s is not a UNIX socket", f.Name())
			}
			if minimumRequestRate < 0 {
				return fmt.Errorf("Invalid value for minRequestRate: %d", minimumRequestRate)
			}
			p, err := os.OpenFile(prometheusFile, os.O_RDWR, 0664)
			if err != nil {
				if os.IsPermission(err) {
					return fmt.Errorf("No write access to %s", prometheusFile)
				}
				return fmt.Errorf("Failed to open file %s for read/write: %v", prometheusFile, err)
			}
			p.Close()

			return exporter.Run(stickTable, socket, minimumRequestRate, prometheusFile)
		},
	}
)

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVarP(&socket, "socket", "s", "/var/lib/haproxy/stats", "Path to the UNIX socket that HAProxy listens on")
	rootCmd.Flags().StringVarP(&prometheusFile, "prometheus-file", "p", "/var/cache/textfile_collector/haproxy_rate_limit_entries.prom", "File to export the generated Prometheus metrics")
	rootCmd.Flags().StringVarP(&stickTable, "stick-table", "t", "table_requests_limiter_src_ip", "Name of the stick-table to query for entries")
	rootCmd.Flags().IntVarP(&minimumRequestRate, "minimum-request-rate", "m", 1, "Minimum request rate for a client IP to be included in the Prometheus metric")
}
