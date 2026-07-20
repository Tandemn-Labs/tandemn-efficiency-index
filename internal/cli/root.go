package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"github.com/tandemn-labs/tandemn-efficiency-index/internal/control"
	"github.com/tandemn-labs/tandemn-efficiency-index/internal/tui"
)

var version = "dev"

type options struct {
	apiURL    string
	token     string
	timeout   time.Duration
	kube      bool
	namespace string
	release   string
	service   string
	localPort int
	json      bool
}

type connection struct {
	client       *control.Client
	dashboardURL string
	portForward  *control.PortForward
}

var (
	accentStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#D97757")).Bold(true)
	mutedStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#7C7C7C"))
	goodStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#70B88A")).Bold(true)
)

func Execute() error {
	return newRootCommand().Execute()
}

func newRootCommand() *cobra.Command {
	opts := &options{}
	root := &cobra.Command{
		Use:           "tei",
		Short:         "A polished control plane for Tandemn Efficiency Index",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConnection(opts, func(conn connection) error {
				return tui.Run(conn.client, conn.dashboardURL)
			})
		},
	}
	root.SetHelpCommand(&cobra.Command{Hidden: true})

	root.PersistentFlags().StringVar(&opts.apiURL, "api-url", env("TEI_API_URL", "http://127.0.0.1:8000"), "direct TEI API URL")
	root.PersistentFlags().StringVar(&opts.token, "token", env("TEI_API_TOKEN", env("TEI_API_BEARER_TOKEN", "")), "TEI API bearer token")
	root.PersistentFlags().DurationVar(&opts.timeout, "timeout", 10*time.Second, "API and port-forward timeout")
	root.PersistentFlags().BoolVar(&opts.kube, "kube", false, "connect through a temporary kubectl port-forward")
	root.PersistentFlags().StringVar(&opts.namespace, "namespace", "tandemn-system", "Kubernetes namespace")
	root.PersistentFlags().StringVar(&opts.release, "release", "tei", "Helm release name")
	root.PersistentFlags().StringVar(&opts.service, "service", "", "Kubernetes Service name override")
	root.PersistentFlags().IntVar(&opts.localPort, "local-port", 0, "local port for --kube; zero selects an available port")

	root.AddCommand(
		newShellCommand(opts),
		newStatusCommand(opts),
		newHealthCommand(opts),
		newReadyCommand(opts),
		newWorkloadsCommand(opts),
		newSnapshotCommand(opts),
		newObserveCommand(opts),
		newDashboardCommand(opts),
		newLogsCommand(opts),
	)
	return root
}

func newShellCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "shell",
		Short: "Open the interactive TEI control room",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConnection(opts, func(conn connection) error {
				return tui.Run(conn.client, conn.dashboardURL)
			})
		},
	}
}

func newStatusCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "status",
		Short: "Show lifecycle and dependency status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConnection(opts, func(conn connection) error {
				payload, err := conn.client.Status()
				if err != nil {
					return err
				}
				printResponse("TEI STATUS", payload, opts.json)
				return nil
			})
		},
	}
	command.Flags().BoolVar(&opts.json, "json", false, "print machine-readable JSON")
	return command
}

func newHealthCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "health",
		Short: "Check service liveness",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConnection(opts, func(conn connection) error {
				payload, err := conn.client.Health()
				if err != nil {
					return err
				}
				printResponse("TEI HEALTH", payload, opts.json)
				return nil
			})
		},
	}
	command.Flags().BoolVar(&opts.json, "json", false, "print machine-readable JSON")
	return command
}

func newReadyCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "ready",
		Short: "Check collection and dependency readiness",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConnection(opts, func(conn connection) error {
				payload, err := conn.client.Readiness()
				if err != nil {
					return err
				}
				printResponse("TEI READINESS", payload, opts.json)
				return nil
			})
		},
	}
	command.Flags().BoolVar(&opts.json, "json", false, "print machine-readable JSON")
	return command
}

func newWorkloadsCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "workloads",
		Short: "List observed workloads",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConnection(opts, func(conn connection) error {
				payload, err := conn.client.Snapshot(3600, 2)
				if err != nil {
					return err
				}
				if opts.json {
					return printJSON(payload["jobs"])
				}
				fmt.Println(tui.FormatWorkloads(payload))
				return nil
			})
		},
	}
	command.Flags().BoolVar(&opts.json, "json", false, "print machine-readable JSON")
	return command
}

func newSnapshotCommand(opts *options) *cobra.Command {
	var window string
	var maxPoints int
	var output string
	command := &cobra.Command{
		Use:   "snapshot",
		Short: "Fetch a bounded observability report",
		RunE: func(cmd *cobra.Command, args []string) error {
			seconds, err := windowSeconds(window)
			if err != nil {
				return err
			}
			return withConnection(opts, func(conn connection) error {
				payload, err := conn.client.Snapshot(seconds, maxPoints)
				if err != nil {
					return err
				}
				data, err := json.MarshalIndent(payload, "", "  ")
				if err != nil {
					return err
				}
				if output != "" {
					if err := os.WriteFile(output, append(data, '\n'), 0o644); err != nil {
						return fmt.Errorf("write snapshot: %w", err)
					}
					fmt.Println(goodStyle.Render("✓") + " wrote " + output)
					return nil
				}
				fmt.Println(string(data))
				return nil
			})
		},
	}
	command.Flags().StringVar(&window, "window", "1h", "15m, 1h, 6h, 24h, or all")
	command.Flags().IntVar(&maxPoints, "max-points", 180, "maximum points per series")
	command.Flags().StringVarP(&output, "output", "o", "", "write JSON to a file")
	return command
}

func newObserveCommand(opts *options) *cobra.Command {
	observe := &cobra.Command{Use: "observe", Short: "Control periodic observation collection"}
	actions := []struct {
		name string
		run  func(*control.Client) (control.Response, error)
	}{
		{name: "start", run: func(client *control.Client) (control.Response, error) { return client.StartObservation() }},
		{name: "stop", run: func(client *control.Client) (control.Response, error) { return client.StopObservation() }},
		{name: "restart", run: func(client *control.Client) (control.Response, error) { return client.RestartObservation() }},
	}
	for _, action := range actions {
		action := action
		actionCommand := &cobra.Command{
			Use:   action.name,
			Short: strings.ToUpper(action.name[:1]) + action.name[1:] + " observation collection",
			RunE: func(cmd *cobra.Command, args []string) error {
				return withConnection(opts, func(conn connection) error {
					payload, err := action.run(conn.client)
					if err != nil {
						return err
					}
					printResponse("OBSERVATION "+strings.ToUpper(action.name), payload, opts.json)
					return nil
				})
			},
		}
		actionCommand.Flags().BoolVar(&opts.json, "json", false, "print machine-readable JSON")
		observe.AddCommand(actionCommand)
	}
	return observe
}

func newLogsCommand(opts *options) *cobra.Command {
	var follow bool
	var tail int
	var deployment string
	command := &cobra.Command{
		Use:   "logs",
		Short: "Read logs from the Kubernetes TEI deployment",
		RunE: func(cmd *cobra.Command, args []string) error {
			name := deployment
			if name == "" {
				name = opts.release + "-tei"
			}
			arguments := []string{
				"--namespace",
				opts.namespace,
				"logs",
				"deployment/" + name,
				fmt.Sprintf("--tail=%d", tail),
			}
			if follow {
				arguments = append(arguments, "--follow")
			}
			logs := exec.Command("kubectl", arguments...)
			logs.Stdin = os.Stdin
			logs.Stdout = os.Stdout
			logs.Stderr = os.Stderr
			if err := logs.Run(); err != nil {
				return fmt.Errorf("read TEI logs: %w", err)
			}
			return nil
		},
	}
	command.Flags().BoolVarP(&follow, "follow", "f", false, "stream new log lines")
	command.Flags().IntVar(&tail, "tail", 200, "number of recent lines")
	command.Flags().StringVar(&deployment, "deployment", "", "Kubernetes Deployment name override")
	return command
}

func newDashboardCommand(opts *options) *cobra.Command {
	var noOpen bool
	command := &cobra.Command{
		Use:   "dashboard",
		Short: "Open the browser dashboard",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConnection(opts, func(conn connection) error {
				fmt.Println(accentStyle.Render("TEI dashboard") + "  " + conn.dashboardURL)
				if !noOpen {
					if err := openBrowser(conn.dashboardURL); err != nil {
						return err
					}
				}
				if conn.portForward != nil {
					fmt.Println(mutedStyle.Render("Port-forward active · press Ctrl+C to close"))
					waitForInterrupt()
				}
				return nil
			})
		},
	}
	command.Flags().BoolVar(&noOpen, "no-open", false, "print the URL without opening a browser")
	return command
}

func withConnection(opts *options, run func(connection) error) error {
	apiURL := opts.apiURL
	var portForward *control.PortForward
	if opts.kube {
		service := opts.service
		if service == "" {
			service = opts.release + "-tei"
		}
		portForward = &control.PortForward{
			Namespace:  opts.namespace,
			Service:    service,
			LocalPort:  opts.localPort,
			RemotePort: 8000,
			Timeout:    opts.timeout,
		}
		if err := portForward.Start(); err != nil {
			return err
		}
		defer portForward.Stop()
		apiURL = portForward.URL()
	}

	return run(connection{
		client:       control.NewClient(apiURL, opts.token, opts.timeout),
		dashboardURL: apiURL,
		portForward:  portForward,
	})
}

func printResponse(title string, payload control.Response, asJSON bool) {
	if asJSON {
		_ = printJSON(payload)
		return
	}
	fmt.Println(accentStyle.Render(title))
	fmt.Println(tui.FormatStatus(payload))
}

func printJSON(value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func windowSeconds(window string) (int, error) {
	windows := map[string]int{
		"15m": 900,
		"1h":  3600,
		"6h":  21600,
		"24h": 86400,
		"all": 0,
	}
	seconds, ok := windows[strings.ToLower(window)]
	if !ok {
		return 0, fmt.Errorf("window must be one of: 15m, 1h, 6h, 24h, all")
	}
	return seconds, nil
}

func openBrowser(target string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		command = exec.Command("open", target)
	case "windows":
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", target)
	default:
		command = exec.Command("xdg-open", target)
	}
	if err := command.Start(); err != nil {
		return fmt.Errorf("open dashboard: %w", err)
	}
	return nil
}

func waitForInterrupt() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
